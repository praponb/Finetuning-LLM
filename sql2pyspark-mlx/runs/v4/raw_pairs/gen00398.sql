with flattened_attrs as (
    select
        id,
        plan,
        currency,
        discounts,
        array_min(tier_prices) as lowest_price,
        slice(tier_limits, 1, 2) as first_two_limits,
        transform(tier_prices, (p, i) -> round(p * tier_limits[i], 2)) as tier_revenue_caps
    from price_tiers
    where currency in ('EUR', 'THB', 'USD')
)
select
    case when lowest_price >= 33 then 'HIGH' else 'LOW' end as lowest_price_tier,
    currency,
    sort_array(collect_set(plan)) as plan_list,
    count(distinct id) as distinct_ids,
    grouping_id() as grouping_level
from flattened_attrs
group by rollup (case when lowest_price >= 33 then 'HIGH' else 'LOW' end, currency)
