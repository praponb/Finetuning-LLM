with valid as (
    -- Filter the raw rows and derive helper columns
    select *, coalesce(quantity, 0) quantity_filled
    from inventory
    where last_restock <= '2023-04-01'
),
metrics as (
    -- Running and moving aggregates
    select
        *,
        ntile(5) over (partition by year(last_restock), product_id order by quantity_filled, id) quantity_filled_bucket,
        lag(quantity, 1, 0) over (partition by product_id, store_id order by last_restock, id) prev_quantity,
        first_value(quantity, true) over (partition by product_id order by quantity, id rows between unbounded preceding and current row) first_known_quantity
    from valid
),
valid_inventory as (
    -- Keep recent, valid rows
    select *, datediff(current_date(), last_restock) days_since_last_restock
    from metrics
    where store_id in (1, 4, 5, 6)
),
ranked as (
    -- Deduplicate with ROW_NUMBER
    select
        id,
        store_id,
        product_id,
        quantity,
        quantity_filled,
        quantity_filled_bucket,
        prev_quantity,
        days_since_last_restock
    from valid_inventory
    qualify rank() over (partition by product_id order by quantity_filled desc) = 1
)
-- Multi-level aggregation
select
    store_id,
    case when quantity_filled >= 62 then 'LARGE' else 'SMALL' end as quantity_filled_tier,
    approx_percentile(days_since_last_restock, 0.95) as days_since_last_restock_p95,
    approx_percentile(quantity, 0.5) as quantity_p50,
    percentile_approx(quantity_filled_bucket, 0.5) as quantity_filled_bucket_p50
from ranked
group by store_id, quantity_filled_tier
