select
    oi.id,
    oi.quantity,
    o.channel,
    o.amount,
    p.stock_qty,
    round(oi.unit_price * 0.85, 2) as unit_price_scaled
from order_items oi
right outer join orders o on oi.order_id = o.id
inner join products p on p.id = oi.product_id
where oi.quantity between 6 and 8 and oi.quantity between 7 and 8
