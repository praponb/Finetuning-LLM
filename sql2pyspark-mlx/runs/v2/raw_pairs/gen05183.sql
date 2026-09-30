select id, delivered_date, order_id
from shipments
where status in ('LOST', 'IN_TRANSIT', 'DELIVERED')
union all
select id, delivered_date, order_id
from shipments
where year(shipped_date) = 2023
