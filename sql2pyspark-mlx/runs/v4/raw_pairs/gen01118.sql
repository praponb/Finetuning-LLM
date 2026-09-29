select
    id,
    product_id,
    sale_date,
    revenue,
    store_id,
    units,
    sum(revenue) over (partition by product_id order by sale_date, id) as running_revenue,
    units - lag(units, 1) over (partition by store_id order by sale_date, id) as units_change
from store_sales
order by units desc, id desc;
