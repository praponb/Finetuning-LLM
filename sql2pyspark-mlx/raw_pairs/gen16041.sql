select delivered_date
from shipments
intersect
select last_restock
from inventory;
