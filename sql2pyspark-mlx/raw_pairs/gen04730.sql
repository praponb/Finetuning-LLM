select
    order_items.unit_price,
    products.price,
    products.product_name,
    orders.amount
from order_items
join products on order_items.product_id = products.id
join orders on order_items.order_id = orders.id
where order_items.quantity > 3;
