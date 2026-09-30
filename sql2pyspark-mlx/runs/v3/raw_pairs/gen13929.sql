select status, 'order' as source
from orders
where status = 'PENDING'
union all
select status, 'shipment' as source
from shipments
order by source
