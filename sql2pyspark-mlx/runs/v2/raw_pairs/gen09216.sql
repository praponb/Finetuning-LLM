select
    ss.id,
    ss.store_id,
    p.brand,
    p.launch_date,
    s.rating,
    s.supplier_name
from store_sales ss
join products p on p.id = ss.product_id
left join suppliers s on s.id = p.supplier_id
where ss.sale_date between '2023-12-01' and '2025-08-01'
