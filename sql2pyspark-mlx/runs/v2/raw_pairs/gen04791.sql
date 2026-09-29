select
    product_id,
    count(distinct revenue) as unique_products,
    avg(revenue) as avg_revenue,
    max(revenue) as max_revenue
from store_sales
group by product_id;
