with exploded as (
    select
        id,
        plan,
        effective_from,
        currency,
        val
    from price_tiers
    lateral view explode(tier_limits) t as val
)
select
    case when val >= 62 then 'LARGE' else 'SMALL' end as val_tier,
    approx_percentile(val, 0.95) as val_p95,
    size(collect_set(currency)) as num_currencies,
    max(val) as max_val
from exploded
group by case when val >= 62 then 'LARGE' else 'SMALL' end
order by num_currencies nulls last;
