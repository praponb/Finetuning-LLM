select page
from web_events
where page is not null
except
select supplier_name as page
from suppliers
where supplier_name is not null
