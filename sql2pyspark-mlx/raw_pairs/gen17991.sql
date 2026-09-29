select
    id,
    quantity,
    avg(quantity) over w_part as last4_avg_quantity,
    max(quantity) over w_part as last4_max_quantity,
    sum(quantity) over w_part as last4_sum_quantity,
    count(quantity) over w_part as last4_count_quantity
from inventory
where store_id in (3, 4, 6)
window w_part as (partition by store_id order by last_restock asc, id asc rows between 3 preceding and current row)
