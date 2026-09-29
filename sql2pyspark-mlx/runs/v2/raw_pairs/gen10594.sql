select
    id,
    revenue,
    units,
    round(units, 1) as units_rounded
from store_sales
where store_id in (3, 5) and product_id in (4, 10)
