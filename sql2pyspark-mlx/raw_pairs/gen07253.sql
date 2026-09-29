with metrics as (
    -- Running and moving aggregates
    select
        *,
        sum(quantity) over w_grp as last4_sum_quantity,
        avg(quantity) over w_grp as last4_avg_quantity,
        min(quantity) over w_grp as last4_min_quantity,
        ntile(10) over (order by quantity desc, id) as quantity_bucket
    from inventory
    where last_restock between '2024-06-01' and '2024-09-30'
    window w_grp as (partition by product_id, store_id order by quantity, id rows between 3 preceding and current row)
),
cleaned as (
    -- Basic cleansing and defaults
    select
        id,
        store_id,
        product_id,
        quantity,
        last_restock,
        last4_sum_quantity,
        last4_min_quantity,
        quantity_bucket,
        floor(quantity_bucket) as quantity_bucket_whole
    from metrics
    where last_restock >= to_date('2023-04-17')
),
metrics_inventory as (
    -- Window metrics over several different partitions
    select
        id,
        store_id,
        last4_min_quantity,
        quantity_bucket,
        quantity_bucket_whole,
        sum(quantity_bucket_whole) over w_recent as running_sum_quantity_bucket_whole,
        count(quantity_bucket_whole) over w_recent as running_count_quantity_bucket_whole,
        avg(quantity_bucket_whole) over w_recent as running_avg_quantity_bucket_whole,
        ntile(4) over w_hist as last4_min_quantity_bucket
    from cleaned
    window w_recent as (partition by product_id, store_id order by quantity_bucket_whole, id rows between unbounded preceding and current row), w_hist as (partition by date_trunc('WEEK', last_restock) order by last4_min_quantity desc, id)
),
ranked as (
    -- Keep one row per key (latest first)
    select *
    from (
        select *, row_number() over (partition by store_id order by quantity_bucket nulls first, id) as latest_rank
        from metrics_inventory
    ) x
    where latest_rank = 1
),
base as (
    -- Basic cleansing and defaults
    select
        *,
        last4_min_quantity - quantity_bucket as last4_min_quantity_minus_quantity_bucket,
        round(last4_min_quantity_bucket * 0.85, 2) as adjusted_last4_min_quantity_bucket
    from ranked
    where last4_min_quantity > 190
),
with_windows as (
    -- Running and moving aggregates
    select
        *,
        count(running_sum_quantity_bucket_whole) over (partition by store_id order by last4_min_quantity_minus_quantity_bucket, id rows between unbounded preceding and current row) as running_count_running_sum_quantity_bucket_whole,
        sum(running_sum_quantity_bucket_whole) over (partition by store_id order by last4_min_quantity_minus_quantity_bucket, id rows between unbounded preceding and current row) as running_sum_running_sum_quantity_bucket_whole,
        min(running_sum_quantity_bucket_whole) over (partition by store_id order by last4_min_quantity_minus_quantity_bucket, id rows between unbounded preceding and current row) as running_min_running_sum_quantity_bucket_whole,
        row_number() over (partition by store_id order by last4_min_quantity desc, id) as row_seq
    from base
),
metrics_inventory_2 as (
    -- Ranking and gap features
    select
        id,
        store_id,
        last4_min_quantity,
        quantity_bucket,
        last4_min_quantity_minus_quantity_bucket,
        running_count_running_sum_quantity_bucket_whole,
        running_sum_running_sum_quantity_bucket_whole,
        avg(quantity_bucket_whole) over (partition by store_id order by latest_rank, id rows between 2 preceding and current row) as last3_avg_quantity_bucket_whole,
        max(quantity_bucket_whole) over (partition by store_id order by latest_rank, id rows between 2 preceding and current row) as last3_max_quantity_bucket_whole,
        sum(quantity_bucket_whole) over (partition by store_id order by latest_rank, id rows between 2 preceding and current row) as last3_sum_quantity_bucket_whole,
        count(quantity_bucket_whole) over (partition by store_id order by latest_rank, id rows between 2 preceding and current row) as last3_count_quantity_bucket_whole
    from with_windows
),
grouped as (
    -- Multi-level aggregation
    select
        case when last3_avg_quantity_bucket_whole >= 61 then 'LARGE' else 'SMALL' end as last3_avg_quantity_bucket_whole_tier,
        store_id,
        round(avg(quantity_bucket), 2) as avg_quantity_bucket,
        count(distinct store_id) as distinct_store_ids
    from metrics_inventory_2
    group by case when last3_avg_quantity_bucket_whole >= 61 then 'LARGE' else 'SMALL' end, store_id
)
select
    last3_avg_quantity_bucket_whole_tier,
    store_id,
    avg_quantity_bucket,
    distinct_store_ids
from grouped;
