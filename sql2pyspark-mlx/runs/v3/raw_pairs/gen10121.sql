select
    oi.id,
    oi.product_id,
    oi.unit_price,
    o.customer_id,
    p.stock_qty,
    round(o.shipping_fee / o.amount, 2) as amount_with_shipping
from order_items oi
inner join orders o on oi.order_id = o.id
inner join products p on oi.product_id = p.id
order by stock_qty, unit_price desc;
