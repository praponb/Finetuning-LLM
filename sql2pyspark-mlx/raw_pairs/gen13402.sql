with cleaned as (
    -- Filter the raw rows and derive helper columns
    select *, datediff(current_date(), last_restock) as days_since_last_restock
    from inventory
    where product_id in (4, 10)
)
-- Ranking and gap features
select
    id,
    store_id,
    product_id,
    quantity,
    row_number() over (partition by store_id order by quantity desc, id) as row_seq,
    max(days_since_last_restock) over (partition by store_id) as store_id_max_days_since_last_restock,
    ntile(4) over (order by quantity, id) as quantity_bucket
from cleaned;
