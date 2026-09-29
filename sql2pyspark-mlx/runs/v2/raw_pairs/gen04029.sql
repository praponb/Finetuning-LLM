select
    order_items.quantity,
    orders.channel,
    orders.customer_id,
    orders.order_date
from order_items
left outer join orders on order_items.order_id = orders.id;
