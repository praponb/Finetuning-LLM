with recent_inventory as (
    select *
    from inventory
    where store_id in (2, 3) and store_id = 5
),
agg as (
    select store_id, sum(quantity) as total_quantity
    from recent_inventory
    group by store_id
)
select store_id, total_quantity
from agg
order by total_quantity, store_id
limit 5
