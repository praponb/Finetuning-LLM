select
    id,
    store_id,
    sale_date,
    revenue,
    lead(revenue, 2) over (partition by store_id order by sale_date, id) as next_revenue
from store_sales
order by id desc;
