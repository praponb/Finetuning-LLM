select
    ss.sale_date,
    ss.revenue,
    p.supplier_id,
    s.store_name,
    s.city,
    array_contains(p.tags, 'premium') as is_premium
from store_sales ss
inner join products p on ss.product_id = p.id
inner join stores s on s.id = ss.store_id
where ss.store_id in (1, 2, 4, 6) and s.city = 'Berlin';
