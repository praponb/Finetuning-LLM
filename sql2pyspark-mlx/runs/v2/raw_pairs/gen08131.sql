select
    id,
    product_id,
    revenue,
    sale_date,
    row_number() over (partition by product_id order by revenue, id) as row_num,
    round(avg(revenue) over (partition by product_id order by sale_date, id rows between 3 preceding and current row), 2) as revenue_moving_avg_4
from store_sales
where product_id in (4, 8);
