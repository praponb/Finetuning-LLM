select
    id,
    product_id,
    date_format(sale_date, 'yyyy-MM-dd') sale_date_label,
    least(units, revenue) least_units_revenue
from store_sales;
