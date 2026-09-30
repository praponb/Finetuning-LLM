select
    ss.units,
    ss.store_id,
    p.stock_qty,
    p.category,
    s.country,
    s.supplier_name
from store_sales ss
left outer join products p on ss.product_id = p.id
left outer join suppliers s on p.supplier_id = s.id
where p.stock_qty < 211 and s.city = 'Bangkok'
