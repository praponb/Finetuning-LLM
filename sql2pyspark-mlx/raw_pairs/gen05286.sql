with parsed as (
    -- Struct fields, map lookups and JSON extraction
    select
        id,
        plan,
        effective_from,
        currency,
        transform(tier_prices, (p, i) -> round(p * tier_limits[i], 2)) as tier_revenue_caps,
        zip_with(tier_prices, discounts, (p, d) -> round(p * (1 - d), 2)) as net_prices,
        aggregate(tier_prices, cast(0 as double), (acc, p) -> acc + p, acc -> round(acc / size(tier_prices), 2)) as avg_tier_price,
        array_min(tier_prices) as lowest_price,
        slice(tier_limits, 1, 2) as first_two_limits
    from price_tiers
)
-- Multi-level aggregation
select
    plan,
    case when avg_tier_price >= 34 then 'PREMIUM' else 'STANDARD' end as avg_tier_price_tier,
    sort_array(collect_set(currency)) as currency_list,
    round(avg(lowest_price), 2) as avg_lowest_price,
    sum(case when plan = 'starter' then lowest_price else 0 end) as starter_lowest_price,
    count(distinct plan) as distinct_plans,
    sum(case when plan = 'enterprise' then avg_tier_price else 0 end) as enterprise_avg_tier_price,
    count_if(currency = 'EUR') as eur_events,
    grouping_id() as grouping_id
from parsed
group by rollup (plan, case when avg_tier_price >= 34 then 'PREMIUM' else 'STANDARD' end);
