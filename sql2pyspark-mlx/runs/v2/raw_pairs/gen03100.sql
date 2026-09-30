select
    oi.id,
    oi.product_id,
    oi.unit_price,
    oi.order_id
from order_items oi
where oi.quantity <= 7 and oi.product_id in (select id from products where stock_qty = 306)
order by order_id desc;
