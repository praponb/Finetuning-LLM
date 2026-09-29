select
    orders.discount,
    orders.customer_id,
    shipments.order_id,
    shipments.cost
from orders
join shipments on shipments.order_id = orders.id;
