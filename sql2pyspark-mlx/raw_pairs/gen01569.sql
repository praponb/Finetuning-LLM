with lines as (
    select
        id,
        currency,
        explode(tier_prices) as tier_value
    from price_tiers
),
filtered as (
    select *, lower(currency) as currency_lower
    from lines
    where currency = 'THB'
),
enriched as (
    select
        currency,
        tier_value,
        cume_dist() over (order by tier_value nulls first) as tier_value_cume_dist,
        sum(tier_value) over (partition by currency) as currency_sum_tier_value
    from filtered
),
cleaned as (
    select *, tier_value + tier_value_cume_dist as tier_value_plus_tier_value_cume_dist
    from enriched
    where currency_sum_tier_value >= 317
),
with_windows as (
    select
        tier_value,
        currency_sum_tier_value,
        percent_rank() over (order by tier_value_plus_tier_value_cume_dist nulls last) as tier_value_plus_tier_value_cume_dist_percent_rank,
        sum(tier_value) over (partition by currency) as currency_sum_tier_value_2
    from cleaned
)
select currency_sum_tier_value, tier_value_plus_tier_value_cume_dist_percent_rank
from with_windows
