select
    id,
    revenue,
    product_id,
    store_id,
    datediff(current_date(), sale_date) as days_since_sale_date
from store_sales
where units between 13 and 40
