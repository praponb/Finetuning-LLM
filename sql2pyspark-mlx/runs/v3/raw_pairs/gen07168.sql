select
    p.id,
    p.category,
    count(ss.id) as store_sale_count,
    round(avg(ss.revenue), 2) as avg_revenue
from products p
left join store_sales ss on p.id = ss.product_id
group by p.id, p.category
