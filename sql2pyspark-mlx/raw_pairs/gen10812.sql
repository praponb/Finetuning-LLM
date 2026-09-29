with filtered as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        case when quantity >= 211 then 'High' when quantity >= 110 then 'Medium' else 'Low' end as quantity_tier,
        date_trunc('WEEK', last_restock) as last_restock_period
    from inventory
    where last_restock >= to_date('2023-08-22')
),
base as (
    -- Keep recent, valid rows
    select
        id,
        product_id,
        last_restock,
        quantity_tier,
        last_restock_period,
        round(quantity * 0.85, 2) as quantity_scaled,
        weekofyear(last_restock) as last_restock_weekofyear
    from filtered
    where last_restock < '2025-02-01'
),
ranked as (
    -- Keep one row per key (latest first)
    select *, row_number() over (partition by product_id order by last_restock_period nulls first, id) as dedup_rank
    from base
    qualify dedup_rank = 1
),
enriched as (
    -- Window metrics over several different partitions
    select
        id,
        product_id,
        last_restock,
        quantity_tier,
        last_restock_period,
        quantity_scaled,
        dedup_rank,
        lag(last_restock_weekofyear) over (partition by product_id order by quantity_scaled, id) as prev_last_restock_weekofyear,
        lag(quantity_scaled) over (partition by product_id order by quantity_scaled, id) as prev_quantity_scaled,
        lead(quantity_scaled) over (partition by product_id order by quantity_scaled, id) as next_quantity_scaled,
        row_number() over (partition by product_id order by quantity_scaled, id) as row_seq,
        count(last_restock_weekofyear) over (partition by product_id order by cast(last_restock_period as bigint) range between 604800 preceding and current row) as last_restock_weekofyear_count_7d
    from ranked
)
-- Multi-level aggregation
select
    product_id,
    date_trunc('month', last_restock) as activity_month,
    count(*) as row_count,
    sum(next_quantity_scaled) as total_next_quantity_scaled,
    min(last_restock_period) as first_seen,
    rank() over (partition by product_id order by count(*) desc) as row_count_rank
from enriched
group by product_id, activity_month;
