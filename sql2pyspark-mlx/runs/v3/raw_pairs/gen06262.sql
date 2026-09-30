select
    id,
    sale_date,
    store_id,
    revenue * units as revenue_per_unit,
    revenue - units as revenue_per_unit_2
from store_sales
where units < 12
