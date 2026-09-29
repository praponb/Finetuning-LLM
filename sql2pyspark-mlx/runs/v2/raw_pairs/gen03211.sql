select
    id,
    product_id,
    units,
    sale_date,
    revenue,
    row_number() over (partition by product_id order by units desc, id) as rn,
    lag(revenue, 2) over (partition by product_id order by sale_date, id) as prev_revenue
from store_sales;
