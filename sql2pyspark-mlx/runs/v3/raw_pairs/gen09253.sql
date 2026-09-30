select
    id,
    store_id,
    sale_date,
    units,
    lag(units, 1) over (partition by store_id order by sale_date, id) as prev_units
from store_sales
where units = 31;
