select
    ss.id,
    ss.product_id,
    ss.store_id,
    p.cost,
    p.launch_date,
    s.rating,
    lower(s.country) country_lower
from store_sales ss
join products p on ss.product_id = p.id
left join suppliers s on s.id = p.supplier_id
where ss.sale_date > '2024-11-01';
