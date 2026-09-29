select
    id,
    product_id,
    store_id,
    quantity,
    last_restock,
    rank() over (partition by product_id, store_id order by quantity desc) as rnk,
    quantity - lag(quantity, 1, 0) over (partition by date_trunc('month', last_restock), product_id order by last_restock, id) as quantity_change
from inventory
where month(last_restock) in (5, 7, 12);
