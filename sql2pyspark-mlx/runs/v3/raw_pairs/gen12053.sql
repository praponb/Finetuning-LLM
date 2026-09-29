select
    id,
    order_id,
    quantity,
    round(quantity / sum(quantity) over (partition by order_id), 3) as quantity_share,
    percent_rank() over (order by quantity) as quantity_pct_rank
from order_items
