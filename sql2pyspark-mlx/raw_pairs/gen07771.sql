with recent_inventory as (
    select *, case when quantity >= 201 then 'High' when quantity >= 78 then 'Medium' else 'Low' end quantity_level
    from inventory
    where quantity is not null and year(last_restock) = 2025
),
store_id_summary as (
    select store_id, product_id, min(last_restock) first_last_restock
    from recent_inventory
    group by store_id, product_id
)
select store_id, product_id, first_last_restock
from store_id_summary
order by product_id desc;
