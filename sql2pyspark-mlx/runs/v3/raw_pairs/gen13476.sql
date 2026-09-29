with filtered_inventory as (
    select *
    from inventory
    where quantity > 74 or product_id in (8, 10)
),
inventory_stats as (
    select store_id, count(distinct quantity) as unique_stores, min(last_restock) as min_last_restock
    from filtered_inventory
    group by store_id
)
select *
from inventory_stats
where unique_stores > 9
