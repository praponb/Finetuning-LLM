with filtered_inventory as (
    select *
    from inventory
    where product_id in (4, 9)
),
agg as (
    select product_id, min(last_restock) as first_last_restock, count(distinct quantity) as unique_stores
    from filtered_inventory
    group by product_id
)
select product_id, first_last_restock, unique_stores
from agg
order by unique_stores desc, product_id
limit 3;
