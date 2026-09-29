with cleaned as (
    -- Keep recent, valid rows
    select
        id,
        store_id,
        product_id,
        quantity,
        last_restock,
        round(quantity, 2) as quantity_rounded,
        ceil(quantity) as quantity_whole
    from inventory
    where last_restock > to_date('2024-03-23')
)
-- Keep one row per key (latest first)
select *
from cleaned
qualify row_number() over (partition by store_id, product_id order by last_restock nulls last, id) = 1
