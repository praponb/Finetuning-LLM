with ranked_inventory as (
    select
        id,
        store_id,
        quantity,
        row_number() over (partition by store_id order by quantity desc, id) as pos
    from inventory
    where quantity between 67 and 232
)
select id, store_id, quantity
from ranked_inventory
where pos <= 5
