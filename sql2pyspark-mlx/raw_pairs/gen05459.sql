select
    id,
    store_id,
    product_id,
    quantity,
    last_restock,
    sum(quantity) over (partition by product_id order by quantity, id rows between unbounded preceding and current row) as running_quantity,
    first_value(quantity) ignore nulls over (order by quantity, id rows between unbounded preceding and current row) as first_known_quantity,
    percent_rank() over (partition by month(last_restock), product_id order by quantity desc nulls last) as quantity_percent_rank
from inventory
