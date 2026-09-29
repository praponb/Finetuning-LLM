with lines as (
    select
        id,
        plan,
        effective_from,
        tier_value
    from price_tiers
    lateral view explode(tier_prices) li as tier_value
),
base as (
    select *, date_trunc('YEAR', effective_from) as effective_from_period
    from lines
    where plan = 'pro'
),
with_windows as (
    select
        plan,
        effective_from,
        tier_value,
        max(tier_value) over (partition by plan) as plan_max_tier_value,
        rank() over (partition by plan order by tier_value nulls first) as tier_value_rank,
        max(tier_value) over (partition by date_trunc('week', effective_from), plan order by cast(effective_from_period as bigint) range between 604800 preceding and current row) as tier_value_max_7d,
        min(tier_value) over (partition by month(effective_from), plan) as effective_from_month_min_tier_value
    from base
),
valid as (
    select
        *,
        floor(effective_from_month_min_tier_value) as effective_from_month_min_tier_value_whole,
        tier_value * effective_from_month_min_tier_value as tier_value_times_effective_from_month_min_tier_value
    from with_windows
    where effective_from > date '2024-03-03'
      and plan = 'basic'
),
scored as (
    select
        plan,
        tier_value_rank,
        tier_value_max_7d,
        effective_from_month_min_tier_value,
        effective_from_month_min_tier_value_whole,
        tier_value_times_effective_from_month_min_tier_value,
        percent_rank() over (partition by month(effective_from), plan order by tier_value_max_7d nulls first) as tier_value_max_7d_percent_rank
    from valid
    where tier_value_rank > 5
)
select *
from (
    select
        plan,
        tier_value_max_7d_percent_rank,
        tier_value_times_effective_from_month_min_tier_value
    from scored
) src
unpivot (
    reading for metric_name in (tier_value_max_7d_percent_rank, tier_value_times_effective_from_month_min_tier_value)
) long_rows;
