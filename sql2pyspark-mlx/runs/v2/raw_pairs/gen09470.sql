select
    ss.id,
    ss.sale_date,
    p.price,
    p.product_name,
    p.brand
from store_sales ss
left join products p on ss.product_id = p.id
where p.category not in ('Grocery', 'Sports', 'Electronics') and p.category in ('Books', 'Grocery', 'Electronics')
order by ss.id asc
limit 20;
