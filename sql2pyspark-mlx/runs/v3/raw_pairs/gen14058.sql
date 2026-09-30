select
    oi.id,
    oi.unit_price,
    p.category,
    p.stock_qty,
    s.country
from order_items oi
left join products p on p.id = oi.product_id
inner join suppliers s on p.supplier_id = s.id
where p.supplier_id is not null
