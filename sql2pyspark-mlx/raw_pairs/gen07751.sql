with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        effective_from,
        currency,
        explode_outer(tier_limits) as tier_value
    from price_tiers
),
metrics as (
    -- Ranking and gap features
    select *, min(tier_value) over (partition by month(effective_from)) as effective_from_month_min_tier_value
    from exploded
    where currency in ('THB', 'EUR', 'USD')
),
enriched as (
    -- Window metrics over several different partitions
    select
        id,
        effective_from,
        avg(effective_from_month_min_tier_value) over (partition by currency) as currency_avg_effective_from_month_min_tier_value
    from metrics
),
kept_keys as (
    -- Set intersection on keys
    select id
    from enriched
    intersect all
    select id
    from price_tiers
    where currency = 'THB'
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from enriched s
    join kept_keys k on s.id = k.id
)
-- Aggregate with conditional metrics
select
    date_trunc('year', effective_from) as year_start,
    percentile_approx(currency_avg_effective_from_month_min_tier_value, 0.5) as currency_avg_effective_from_month_min_tier_value_p50,
    avg(currency_avg_effective_from_month_min_tier_value) as avg_currency_avg_effective_from_month_min_tier_value,
    max(currency_avg_effective_from_month_min_tier_value) as max_currency_avg_effective_from_month_min_tier_value,
    sum(currency_avg_effective_from_month_min_tier_value) as total_currency_avg_effective_from_month_min_tier_value,
    percentile_approx(currency_avg_effective_from_month_min_tier_value, array(0.25, 0.5, 0.75)) as currency_avg_effective_from_month_min_tier_value_quartiles,
    approx_percentile(currency_avg_effective_from_month_min_tier_value, array(0.25, 0.5, 0.75)) as currency_avg_effective_from_month_min_tier_value_quartiles_2
from reconciled
group by date_trunc('year', effective_from);
