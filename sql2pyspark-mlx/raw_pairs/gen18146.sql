with top_inventory as (
    select
        id,
        store_id,
        quantity,
        rank() over (partition by store_id order by quantity) as pos
    from inventory
    where last_restock >= '2023-09-01'
)
select id, store_id, quantity
from top_inventory
where pos <= 1
