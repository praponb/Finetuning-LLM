select carrier, delivered_date
from shipments
where status in ('IN_TRANSIT', 'DELIVERED')
union all
select carrier, delivered_date
from shipments
where order_id in (11, 19, 40);
