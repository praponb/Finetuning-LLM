select
    ss.id,
    ss.units,
    ss.product_id,
    p.supplier_id,
    s.supplier_name
from store_sales ss
inner join products p on ss.product_id = p.id
inner join suppliers s on p.supplier_id = s.id
where p.category not in ('Electronics', 'Books', 'Sports');
