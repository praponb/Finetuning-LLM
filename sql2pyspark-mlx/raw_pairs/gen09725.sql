with cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        store_id,
        product_id,
        quantity,
        last_restock,
        datediff(current_date(), last_restock) as days_since_last_restock
    from inventory
    where last_restock > to_date('2023-08-04')
),
metrics as (
    -- Ranking and gap features
    select
        *,
        min(quantity) over (partition by store_id) as store_id_min_quantity,
        row_number() over (partition by product_id order by quantity, id) as seq_no
    from cleaned
),
enriched as (
    -- Window metrics over several different partitions
    select
        id,
        product_id,
        store_id_min_quantity,
        seq_no,
        lead(store_id_min_quantity, 1) over (order by quantity, id) as next_store_id_min_quantity,
        avg(quantity) over (order by last_restock, id rows between 2 preceding and 1 preceding) as quantity_avg_2_prior,
        ntile(5) over (partition by month(last_restock), product_id order by store_id_min_quantity, id) as store_id_min_quantity_bucket
    from metrics
    where seq_no = 5
),
enriched_inventory as (
    -- Ranking and gap features
    select
        *,
        round(store_id_min_quantity_bucket / sum(store_id_min_quantity_bucket) over (partition by product_id), 4) as store_id_min_quantity_bucket_share,
        lag(next_store_id_min_quantity, 2) over (partition by product_id order by next_store_id_min_quantity, id) as prev_next_store_id_min_quantity
    from enriched
)
select
    id,
    product_id,
    store_id_min_quantity,
    seq_no,
    next_store_id_min_quantity,
    store_id_min_quantity_bucket,
    prev_next_store_id_min_quantity
from enriched_inventory
