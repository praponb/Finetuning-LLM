select
    id,
    product_id,
    sale_date,
    units,
    round(avg(units) over (partition by product_id order by sale_date, id rows between 6 preceding and current row), 2) as units_moving_avg_7
from store_sales
