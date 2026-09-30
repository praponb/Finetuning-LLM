select
    id,
    product_id,
    revenue,
    sale_date,
    store_id,
    first_value(sale_date) over (partition by product_id order by revenue, id) as first_sale_date,
    lead(revenue, 1) over (partition by store_id order by sale_date, id) as next_revenue
from store_sales
where units between 22 and 29;
