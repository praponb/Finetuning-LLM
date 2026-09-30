with base as (
    -- Keep recent, valid rows
    select *, floor(quantity) quantity_whole
    from inventory
    where last_restock <= '2024-12-01'
),
with_windows as (
    -- Window metrics over several different partitions
    select
        *,
        percent_rank() over w_part quantity_whole_percent_rank,
        ntile(10) over w_recent quantity_whole_bucket,
        rank() over (partition by store_id order by quantity nulls last) quantity_rank
    from base
    window w_part as (partition by store_id order by quantity_whole desc nulls first), w_recent as (order by quantity_whole desc, id)
),
valid as (
    -- Keep recent, valid rows
    select
        id,
        store_id,
        product_id,
        quantity,
        quantity_whole,
        quantity_whole_percent_rank,
        quantity_whole_bucket,
        quantity_rank,
        quantity_whole_bucket * quantity_rank quantity_whole_bucket_times_quantity_rank,
        quantity_whole + quantity_whole_percent_rank quantity_whole_plus_quantity_whole_percent_rank
    from with_windows
    where quantity_rank between 4 and 7
),
scored as (
    -- Running and moving aggregates
    select
        id,
        store_id,
        product_id,
        quantity,
        quantity_whole,
        quantity_whole_percent_rank,
        quantity_whole_bucket,
        quantity_rank,
        quantity_whole_bucket_times_quantity_rank,
        quantity_whole_plus_quantity_whole_percent_rank,
        lead(quantity_whole, 1) over (partition by store_id order by quantity_whole_bucket_times_quantity_rank, id) next_quantity_whole,
        lag(quantity_whole_plus_quantity_whole_percent_rank, 1, 0) over (order by quantity_whole_bucket_times_quantity_rank, id) prev_quantity_whole_plus_quantity_whole_percent_rank,
        avg(quantity) over w_hist product_id_avg_quantity
    from valid
    window w_hist as (partition by product_id, store_id)
),
scored_inventory as (
    -- Ranking and gap features
    select
        *,
        ntile(5) over (partition by store_id order by quantity_whole_bucket, id) quantity_whole_bucket_bucket,
        lead(product_id_avg_quantity, 1) over (partition by store_id order by quantity_rank, id) next_product_id_avg_quantity,
        ntile(5) over (partition by store_id order by prev_quantity_whole_plus_quantity_whole_percent_rank, id) prev_quantity_whole_plus_quantity_whole_percent_rank_bucket,
        first_value(product_id_avg_quantity, true) over (partition by store_id, product_id order by quantity_whole_bucket, id rows between unbounded preceding and current row) first_known_product_id_avg_quantity
    from scored
),
deduped as (
    -- Deduplicate with ROW_NUMBER
    select *
    from (
        select *, row_number() over (partition by product_id order by quantity_whole_bucket_times_quantity_rank desc nulls last, id) as rank_in_group
        from scored_inventory
    ) x
    where rank_in_group = 1
),
deduped_inventory as (
    -- Keep one row per key (latest first)
    select *
    from (
        select *, rank() over (partition by store_id order by next_product_id_avg_quantity desc nulls first) as rank_in_group_2
        from deduped
    ) x
    where rank_in_group_2 = 1
),
totals as (
    -- Multi-level aggregation
    select
        case when quantity_whole_percent_rank >= 0.7 then 'PREMIUM' else 'STANDARD' end as quantity_whole_percent_rank_tier,
        product_id,
        bit_and(quantity) as quantity_bit_and,
        count(distinct id) as distinct_ids,
        approx_percentile(quantity_whole_bucket, 0.9) as quantity_whole_bucket_p90,
        max(quantity_rank) as max_quantity_rank,
        bit_or(quantity) as quantity_bit_or,
        grouping_id() as grouping_id
    from deduped_inventory
    group by cube (case when quantity_whole_percent_rank >= 0.7 then 'PREMIUM' else 'STANDARD' end, product_id)
)
select
    quantity_whole_percent_rank_tier,
    quantity_bit_and,
    distinct_ids,
    quantity_whole_bucket_p90,
    quantity_bit_or,
    grouping_id
from totals
order by quantity_whole_bucket_p90 nulls first
