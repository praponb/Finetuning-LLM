select
    id,
    store_id,
    revenue,
    round(revenue / sum(revenue) over (partition by store_id), 3) revenue_share
from store_sales
where units between 17 and 23
