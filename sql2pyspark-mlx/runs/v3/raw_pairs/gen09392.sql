select
    oi.id,
    oi.unit_price,
    oi.product_id,
    p.stock_qty,
    p.category,
    s.country,
    s.rating
from order_items oi
inner join products p on oi.product_id = p.id
inner join suppliers s on p.supplier_id = s.id
