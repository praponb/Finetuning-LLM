select
    id,
    store_id,
    sale_date,
    revenue,
    sum(revenue) over (partition by store_id order by sale_date, id) running_revenue
from store_sales
where year(sale_date) = 2025;
