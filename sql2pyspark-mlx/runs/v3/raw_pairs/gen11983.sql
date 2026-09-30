select
    id,
    store_id,
    last_restock,
    quantity,
    round(avg(quantity) over (partition by store_id order by last_restock, id rows between 6 preceding and current row), 2) as quantity_moving_avg_7
from inventory
order by last_restock;
