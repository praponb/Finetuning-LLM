select
    ss.id,
    ss.store_id,
    ss.product_id,
    p.product_name,
    p.supplier_id,
    s.region,
    s.store_name
from store_sales ss
left join products p on ss.product_id = p.id
inner join stores s on ss.store_id = s.id
where ss.store_id in (1, 5, 6) or p.category not in ('Grocery', 'Books', 'Home');
