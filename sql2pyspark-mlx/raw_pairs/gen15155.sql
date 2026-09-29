select
    product_id,
    store_id,
    sale_date,
    revenue - units as revenue_per_unit
from store_sales
where units < 13
