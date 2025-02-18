# testgit-new branch 
test git commands
test git commands @copyright
#text added 4:45
#text aded 5:25
#text aded 5 May +++

new line added removing old
from pydantic import BaseModel, Field
from typing import Dict, Any, Optional

class DatasetMetadata(BaseModel):
   dataset_id: str
   dataset_name: str
   schema: Optional[str] = None

class AttributeMetadata(BaseModel):
   attributes: Dict[str, Any]

class MappingMetadata(BaseModel):
   mappings: Dict[str, Any]

class SourceMetadata(BaseModel):
   source_details: Dict[str, Any]

class MetadataSchema(BaseModel):
   dataset: DatasetMetadata
   attributes: AttributeMetadata
   mappings: MappingMetadata
   source: SourceMetadata

-----------------------------------------------------------------------------
import logging
from external.mdr_service import MDR_Service
from .metadata_schema import MetadataSchema

# Configure Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MetadataHandler:
   """Base class for metadata handlers."""
   def __init__(self, next_handler=None):
       self.next_handler = next_handler

   def handle(self, metadata):
       if self.next_handler:
           return self.next_handler.handle(metadata)
       return metadata

class DatasetMetadataHandler(MetadataHandler):
   """Fetch dataset metadata and pass it to the next handler."""
   def __init__(self, next_handler=None):
       super().__init__(next_handler)
       self.mdr = MDR_Service()

   def handle(self, request):
       logger.info(f"Fetching dataset metadata for {request['target_dataset']}")
       request['dataset'] = self.mdr.get_dataset_metadata(request['target_dataset'])
       return super().handle(request)

class AttributeMetadataHandler(MetadataHandler):
   """Fetch attribute metadata based on dataset metadata."""
   def __init__(self, next_handler=None):
       super().__init__(next_handler)
       self.mdr = MDR_Service()

   def handle(self, request):
       logger.info("Fetching attribute metadata...")
       request['attributes'] = self.mdr.get_attribute_metadata(request['dataset']['dataset_id'])
       return super().handle(request)

class MappingMetadataHandler(MetadataHandler):
   """Fetch mapping metadata based on attribute metadata."""
   def __init__(self, next_handler=None):
       super().__init__(next_handler)
       self.mdr = MDR_Service()

   def handle(self, request):
       logger.info("Fetching mapping metadata...")
       request['mappings'] = self.mdr.get_mapping_metadata(request['dataset']['dataset_id'])
       return super().handle(request)

class SourceMetadataHandler(MetadataHandler):
   """Fetch source metadata based on mapping metadata."""
   def __init__(self, next_handler=None):
       super().__init__(next_handler)
       self.mdr = MDR_Service()

   def handle(self, request):
       logger.info("Fetching source metadata...")
       request['source'] = self.mdr.get_source_metadata(request['dataset']['dataset_id'])
       return super().handle(request)

--------------------------------------------------------------


import json
from .metadata_chain import (
   DatasetMetadataHandler,
   AttributeMetadataHandler,
   MappingMetadataHandler,
   SourceMetadataHandler
)
from .metadata_schema import MetadataSchema

class MDRServiceFacade:
   """
   Facade to trigger metadata fetching chain.
   """
   def __init__(self):
       self.chain = DatasetMetadataHandler(
           AttributeMetadataHandler(
               MappingMetadataHandler(
                   SourceMetadataHandler()
               )
           )
       )

   def get_metadata(self, target_dataset):
       """
       Calls metadata handlers in sequence and returns validated JSON.
       """
       request = {"target_dataset": target_dataset}
       metadata = self.chain.handle(request)

       # Validate metadata using Pydantic
       validated_metadata = MetadataSchema(**metadata)
       return validated_metadata.dict()

   def get_metadata_json(self, target_dataset):
       return json.dumps(self.get_metadata(target_dataset), indent=4)


--------------------------------------------------------------
