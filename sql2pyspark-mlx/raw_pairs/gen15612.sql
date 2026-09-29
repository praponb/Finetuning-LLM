with lines as (
    select
        id,
        currency,
        explode(discounts) as tier_value
    from price_tiers
),
base as (
    select *, round(tier_value * 1.1, 2) as tier_value_adjusted
    from lines
    where currency = 'USD'
),
metrics as (
    select
        currency,
        tier_value_adjusted,
        min(tier_value_adjusted) over w_part as currency_min_tier_value_adjusted,
        rank() over w_ordered as tier_value_adjusted_rank,
        percent_rank() over (partition by currency order by tier_value_adjusted asc nulls last) as tier_value_adjusted_percent_rank
    from base
    window w_part as (partition by currency), w_ordered as (partition by currency order by tier_value_adjusted asc nulls first)
),
matched_keys as (
    select currency
    from metrics
    intersect all
    select currency
    from price_tiers
    where month(effective_from) in (1, 11, 12)
),
kept_rows as (
    select s.*
    from metrics s
    inner join matched_keys k on s.currency = k.currency
),
enriched as (
    select
        currency,
        tier_value_adjusted,
        currency_min_tier_value_adjusted,
        tier_value_adjusted_rank,
        min(currency_min_tier_value_adjusted) over (partition by currency) as currency_min_currency_min_tier_value_adjusted,
        cume_dist() over (order by currency_min_tier_value_adjusted desc nulls last) as currency_min_tier_value_adjusted_cume_dist
    from kept_rows
    where currency_min_tier_value_adjusted >= 24
)
select
    case when tier_value_adjusted_rank >= 7 then 'LARGE' else 'SMALL' end as tier_value_adjusted_rank_tier,
    count(case when currency = 'USD' then 1 end) as usd_count,
    count(*) as row_count,
    count(distinct currency) as distinct_currencies
from enriched
group by tier_value_adjusted_rank_tier
