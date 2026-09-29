select
    id,
    sale_date,
    revenue,
    units,
    revenue * units as revenue_per_unit,
    round(units, 0) as units_rounded
from store_sales
order by id desc, sale_date asc;
