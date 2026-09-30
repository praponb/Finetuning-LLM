select
    oi.product_id,
    oi.order_id,
    p.brand,
    s.country,
    s.city
from order_items oi
inner join products p on oi.product_id = p.id
inner join suppliers s on p.supplier_id = s.id
