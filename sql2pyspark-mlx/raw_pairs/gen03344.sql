select
    id,
    product_id,
    quantity,
    last_restock,
    last_value(quantity) ignore nulls over (partition by store_id, product_id order by quantity asc, id asc rows between unbounded preceding and current row) as last_known_quantity,
    row_number() over (partition by year(last_restock) order by last_restock desc, id asc) as event_seq
from inventory;
