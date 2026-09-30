with lines as (
    select
        id,
        plan,
        effective_from,
        currency,
        val
    from price_tiers
    lateral view outer explode(tier_limits) x as val
)
select
    currency,
    case when val >= 84 then 'PREMIUM' else 'STANDARD' end as val_tier,
    percentile_approx(val, 0.9) as val_p90,
    count_if(plan = 'enterprise') as enterprise_events,
    sort_array(collect_set(plan)) as plan_list,
    sort_array(collect_set(plan)) as plan_list_2,
    min(val) as min_val
from lines
group by currency, case when val >= 84 then 'PREMIUM' else 'STANDARD' end;
