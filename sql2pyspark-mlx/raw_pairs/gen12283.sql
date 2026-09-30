select id, status, shipped_date
from shipments
where cost < 89
union all
select id, status, shipped_date
from shipments
where order_id is not null
