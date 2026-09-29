select
    id,
    product_id,
    revenue,
    add_months(sale_date, 12) as sale_date_plus_months
from store_sales
