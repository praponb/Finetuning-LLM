select
    id,
    revenue,
    store_id,
    datediff(current_date(), sale_date) as days_since_sale_date,
    date_format(sale_date, 'MMM yyyy') as sale_date_label
from store_sales;
