with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        plan,
        currency,
        explode_outer(tier_prices) as val
    from price_tiers
)
-- Multi-level aggregation
select
    case when val >= 75 then 'LARGE' else 'SMALL' end as val_tier,
    currency,
    sort_array(collect_set(currency)) as currency_list,
    percentile_approx(val, 0.5) as val_p50,
    count(case when currency = 'EUR' then 1 end) as eur_count,
    avg(val) as avg_val,
    size(collect_set(plan)) as num_plans,
    percent_rank() over (partition by case when val >= 75 then 'LARGE' else 'SMALL' end order by count(case when currency = 'EUR' then 1 end) desc) as eur_count_percent_rank
from exploded
group by case when val >= 75 then 'LARGE' else 'SMALL' end, currency;
