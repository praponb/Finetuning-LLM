select
    id,
    revenue,
    product_id,
    cast(revenue as int) revenue_int
from store_sales
where store_id in (4, 6);
