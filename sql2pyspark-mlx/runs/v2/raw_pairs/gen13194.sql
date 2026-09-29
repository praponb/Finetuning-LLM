select
    oi.order_id,
    oi.product_id,
    o.store_id,
    o.shipping_fee,
    p.price,
    p.launch_date
from order_items oi
full outer join orders o on o.id = oi.order_id
inner join products p on oi.product_id = p.id
where o.shipping_fee >= 31 and p.stock_qty = 169
