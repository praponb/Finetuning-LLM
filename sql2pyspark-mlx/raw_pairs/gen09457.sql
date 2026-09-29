select payments.paid_at, payments.status, orders.amount
from payments
left outer join orders on orders.id = payments.order_id
where orders.store_id is not null
order by payments.paid_at;
