select
    id,
    store_id,
    units,
    revenue,
    round(revenue, 0) as revenue_rounded
from store_sales
where revenue > 3000
order by store_id desc;
