select
    s.city,
    p.category,
    p.price,
    p.product_name
from suppliers s
left join products p on s.id = p.supplier_id
where s.country in ('UK', 'SG', 'DE') and month(p.launch_date) in (1, 3, 4);
