select
    order_items.product_id,
    order_items.unit_price,
    orders.status,
    orders.store_id,
    products.stock_qty
from order_items
join orders on order_items.order_id = orders.id
join products on products.id = order_items.product_id
where order_items.product_id is null
order by order_items.unit_price;
