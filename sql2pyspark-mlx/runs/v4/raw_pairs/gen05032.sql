with lines as (
    -- Unnest the array into one row per element
    select
        id,
        effective_from,
        currency,
        tier_value
    from price_tiers
    lateral view outer explode(tier_prices) x as tier_value
)
-- Grouped metrics
select
    currency,
    case when tier_value >= 71 then 'HIGH' else 'LOW' end as tier_value_tier,
    count(*) as event_count,
    sum(tier_value) as total_tier_value,
    count(*) filter (where currency = 'EUR') as eur_rows,
    sort_array(collect_set(currency)) as currency_list,
    sort_array(collect_set(currency)) as currency_list_2,
    grouping(currency) as gid
from lines
group by cube (currency, case when tier_value >= 71 then 'HIGH' else 'LOW' end)
having count(*) >= 1
