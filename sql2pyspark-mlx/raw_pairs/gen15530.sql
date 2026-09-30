select
    order_items.id,
    order_items.quantity,
    orders.amount,
    orders.channel,
    products.price,
    ceil(order_items.unit_price) as unit_price_whole
from order_items
inner join orders on orders.id = order_items.order_id
inner join products on products.id = order_items.product_id
order by id desc
