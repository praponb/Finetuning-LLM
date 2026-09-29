select
    oi.id,
    oi.quantity,
    oi.product_id,
    p.price,
    s.country
from order_items oi
inner join products p on oi.product_id = p.id
left outer join suppliers s on s.id = p.supplier_id
where s.country in ('JP', 'US', 'DE') and s.country != 'JP'
