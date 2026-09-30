select
    ss.id,
    ss.revenue,
    p.supplier_id,
    s.country
from store_sales ss
right join products p on ss.product_id = p.id
left join suppliers s on p.supplier_id = s.id
where p.launch_date >= '2023-04-17';
