with valid_inventory as (
    select *
    from inventory
    where year(last_restock) >= 2024
),
summary as (
    select store_id, min(last_restock) as min_last_restock, count(quantity) as quantity_count
    from valid_inventory
    group by store_id
)
select *
from summary;
