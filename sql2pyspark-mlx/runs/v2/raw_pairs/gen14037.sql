select
    id,
    store_id,
    product_id,
    quantity,
    percent_rank() over (partition by store_id, product_id order by quantity desc) as quantity_pct_rank
from inventory
order by quantity
