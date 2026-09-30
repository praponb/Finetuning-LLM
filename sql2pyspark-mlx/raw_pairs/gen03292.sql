with lines as (
    select
        id,
        plan,
        effective_from,
        val
    from price_tiers
    lateral view explode(tier_limits) li as val
),
base as (
    select
        *,
        add_months(effective_from, 3) effective_from_plus_months,
        lower(plan) plan_lower
    from lines
    where plan != 'pro'
),
valid as (
    select
        *,
        datediff(effective_from, effective_from_plus_months) / 7 weeks_effective_from_plus_months_to_effective_from,
        case when plan = 'basic' then 1 else 0 end is_basic
    from base
    where month(effective_from) in (1, 11, 12)
),
cleaned as (
    select *, val / nullif(weeks_effective_from_plus_months_to_effective_from, 0) val_per_weeks_effective_from_plus_months_to_effective_from
    from valid
    where effective_from > date '2024-02-26'
),
base_price_tiers as (
    select
        id,
        plan,
        effective_from,
        effective_from_plus_months,
        weeks_effective_from_plus_months_to_effective_from,
        is_basic,
        val_per_weeks_effective_from_plus_months_to_effective_from,
        quarter(effective_from) effective_from_quarter
    from cleaned
    where effective_from > '2023-08-11'
)
select
    case when val_per_weeks_effective_from_plus_months_to_effective_from >= 72 then 'HIGH' else 'LOW' end as val_per_weeks_effective_from_plus_months_to_effective_from_tier,
    approx_percentile(val_per_weeks_effective_from_plus_months_to_effective_from, array(0.25, 0.5, 0.75)) as val_per_weeks_effective_from_plus_months_to_effective_from_quartiles,
    approx_percentile(effective_from_quarter, 0.95) as effective_from_quarter_p95,
    count(*) filter (where plan = 'enterprise') as enterprise_rows,
    sum(case when plan = 'basic' then effective_from_quarter else 0 end) as basic_effective_from_quarter,
    round(avg(is_basic), 2) as avg_is_basic,
    approx_percentile(weeks_effective_from_plus_months_to_effective_from, 0.9) as weeks_effective_from_plus_months_to_effective_from_p90
from base_price_tiers
group by rollup (case when val_per_weeks_effective_from_plus_months_to_effective_from >= 72 then 'HIGH' else 'LOW' end);
