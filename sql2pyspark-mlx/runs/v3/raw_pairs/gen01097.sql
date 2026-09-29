with numbered as (
    select
        id,
        store_id,
        last_restock,
        product_id,
        dense_rank() over (partition by store_id order by last_restock desc) as rn
    from inventory
)
select
    id,
    store_id,
    last_restock,
    product_id
from numbered
where rn <= 2;
