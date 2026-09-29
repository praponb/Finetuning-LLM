select
    id,
    store_id,
    sale_date,
    units,
    round(avg(units) over (partition by store_id order by sale_date, id rows between 3 preceding and current row), 2) as units_moving_avg_4
from store_sales
