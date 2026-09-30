select
    id,
    store_id,
    revenue,
    row_number() over (partition by store_id order by revenue desc, id) as rn
from store_sales
