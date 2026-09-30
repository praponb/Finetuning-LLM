with filtered as (
    select
        id,
        store_id,
        quantity,
        last_restock,
        add_months(last_restock, 1) as last_restock_plus_months
    from inventory
    where last_restock > date '2023-05-26'
      and last_restock < '2025-10-30'
)
select store_id, id
from filtered
intersect all
select store_id, id
from inventory
where quantity <= 187;
