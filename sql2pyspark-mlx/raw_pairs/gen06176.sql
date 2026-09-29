select
    p.id,
    p.category,
    count(ss.id) as store_sale_count,
    count(ss.revenue) as revenue_count
from products p
left join store_sales ss on p.id = ss.product_id
group by p.id, p.category
order by store_sale_count desc;
