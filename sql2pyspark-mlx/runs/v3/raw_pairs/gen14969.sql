select o.id, o.amount, o.status
from orders o
where o.store_id is null and o.id in (select order_id from shipments)
order by id
limit 1
