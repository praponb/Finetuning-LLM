with scored as (
    -- Window metrics over several different partitions
    select
        *,
        max(quantity) over (partition by product_id) as product_id_max_quantity,
        ntile(10) over (partition by product_id order by quantity, id) as quantity_bucket,
        sum(quantity) over w_hist as running_quantity
    from inventory
    window w_hist as (partition by store_id order by quantity, id rows between unbounded preceding and current row)
)
select
    id,
    store_id,
    product_id,
    quantity,
    last_restock,
    product_id_max_quantity,
    quantity_bucket
from scored;
