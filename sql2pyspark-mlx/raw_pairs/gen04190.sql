select shipped_date as event_date
from shipments
where status = 'LOST'
intersect
select launch_date as event_date
from products
