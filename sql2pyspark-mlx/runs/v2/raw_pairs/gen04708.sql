select
    id,
    product_id,
    quantity,
    store_id,
    last_restock,
    dense_rank() over (partition by product_id order by quantity desc) as dense_rnk,
    dense_rank() over (partition by store_id order by last_restock desc) as dense_rnk_2
from inventory
where last_restock > '2024-06-01'
order by id desc, product_id asc
