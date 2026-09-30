select
    id,
    sale_date,
    product_id,
    units,
    row_number() over (order by sale_date desc, id) as rn,
    round(avg(units) over (partition by product_id order by sale_date, id rows between 6 preceding and current row), 2) as units_moving_avg_7
from store_sales
order by id;
