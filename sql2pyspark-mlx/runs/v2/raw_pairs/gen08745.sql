select orders.id, orders.amount, orders.store_id
from orders
where orders.id in (select order_id from payments)
