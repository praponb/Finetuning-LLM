select
    order_items.quantity,
    order_items.unit_price,
    orders.shipping_fee,
    products.launch_date,
    products.cost
from order_items
inner join orders on order_items.order_id = orders.id
inner join products on order_items.product_id = products.id
where products.supplier_id = 36;
