with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        plan,
        effective_from,
        explode_outer(tier_prices) as tier_value
    from price_tiers
),
base as (
    -- Basic cleansing and defaults
    select *, last_day(effective_from) as effective_from_month_end
    from exploded
    where effective_from < '2025-09-01'
),
base_price_tiers as (
    -- Keep recent, valid rows
    select
        id,
        plan,
        effective_from,
        tier_value,
        date_format(effective_from, 'MMM yyyy') as effective_from_label
    from base
    where effective_from > date '2024-04-14'
      and tier_value > 32
),
cleaned as (
    -- Basic cleansing and defaults
    select
        *,
        date_trunc('MONTH', effective_from) as effective_from_period,
        year(effective_from) as effective_from_year
    from base_price_tiers
    where tier_value >= 21
),
metrics as (
    -- Window metrics over several different partitions
    select *, dense_rank() over (partition by plan order by tier_value desc nulls first) as tier_value_dense_rank
    from cleaned
),
base_price_tiers_2 as (
    -- Keep recent, valid rows
    select
        id,
        plan,
        effective_from,
        tier_value,
        effective_from_label,
        effective_from_period,
        effective_from_year,
        tier_value_dense_rank,
        date_trunc('YEAR', effective_from_period) as effective_from_period_period
    from metrics
    where effective_from <= '2025-09-01'
),
scored as (
    -- Running and moving aggregates
    select
        *,
        percent_rank() over (partition by plan order by tier_value_dense_rank) as tier_value_dense_rank_percent_rank,
        dense_rank() over (partition by date_trunc('month', effective_from_period), plan order by effective_from_year nulls last) as effective_from_year_dense_rank
    from base_price_tiers_2
    where tier_value <= 61
)
select
    id,
    plan,
    effective_from,
    effective_from_label,
    effective_from_period_period,
    tier_value_dense_rank_percent_rank,
    effective_from_year_dense_rank
from scored
order by effective_from_year_dense_rank, effective_from
