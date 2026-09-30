select
    id,
    sale_date,
    store_id,
    revenue,
    rank() over (order by sale_date) as rnk,
    round(revenue / sum(revenue) over (partition by store_id), 3) as revenue_share
from store_sales
order by store_id desc;
