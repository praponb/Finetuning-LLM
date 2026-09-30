select
    id,
    product_id,
    revenue,
    sale_date,
    sum(revenue) over (partition by product_id) as product_id_sum_revenue,
    lag(revenue, 1, 0) over (order by sale_date, id) as prev_revenue
from store_sales
where revenue > 3500;
