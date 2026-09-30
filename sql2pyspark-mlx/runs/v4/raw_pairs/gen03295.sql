select
    id,
    product_id,
    revenue,
    round(revenue / sum(revenue) over (partition by product_id), 3) as revenue_share
from store_sales
where sale_date >= '2024-11-01'
