select id, location, case when location in ('Chiang Mai', 'New York') then 'Priority' else 'Other' end location_group
from departments
