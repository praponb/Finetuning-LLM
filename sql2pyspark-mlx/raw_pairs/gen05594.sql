select
    id,
    store_id,
    sale_date,
    revenue,
    units,
    sum(revenue) over (partition by store_id order by sale_date, id rows between unbounded preceding and current row) as running_revenue,
    first_value(units) over (partition by store_id order by sale_date, id) as first_units
from store_sales
where store_id = 2
