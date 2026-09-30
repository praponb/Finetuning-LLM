select s.status, o.amount, o.customer_id
from shipments s
left outer join orders o on s.order_id = o.id
where o.customer_id is not null
