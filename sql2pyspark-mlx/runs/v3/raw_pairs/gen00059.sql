select
    oi.unit_price,
    p.category,
    p.stock_qty,
    o.channel,
    o.order_date
from order_items oi
left outer join products p on p.id = oi.product_id
left outer join orders o on oi.order_id = o.id
