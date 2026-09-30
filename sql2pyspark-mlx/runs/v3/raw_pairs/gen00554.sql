select status
from orders
where store_id = 28
intersect
select status
from shipments
where carrier in ('FedEx', 'Kerry', 'UPS')
order by status desc
