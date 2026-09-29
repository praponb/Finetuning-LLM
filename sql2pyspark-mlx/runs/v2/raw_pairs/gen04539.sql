select
    orders.order_date,
    orders.channel,
    orders.discount,
    order_items.product_id,
    order_items.unit_price
from orders
inner join order_items on order_items.order_id = orders.id
where orders.order_date < '2024-02-28' and order_items.unit_price < 110;
