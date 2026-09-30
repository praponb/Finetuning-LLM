select
    oi.id,
    oi.order_id,
    o.shipping_fee,
    p.stock_qty
from order_items oi
left join orders o on oi.order_id = o.id
left join products p on p.id = oi.product_id
order by shipping_fee, p.stock_qty desc
