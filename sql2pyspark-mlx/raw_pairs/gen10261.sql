with exploded as (
    select
        id,
        plan,
        explode_outer(tier_prices) as element
    from price_tiers
),
filtered as (
    select *, cast(element as int) as element_int
    from exploded
    where plan = 'pro'
),
filtered_price_tiers as (
    select *, round(element_int * 0.9, 2) as element_int_scaled
    from filtered
    where plan = 'enterprise'
),
valid as (
    select
        *,
        upper(plan) as plan_upper,
        initcap(trim(plan)) as plan_title
    from filtered_price_tiers
    where element is not null
),
base as (
    select
        id,
        plan,
        element,
        element_int,
        element_int_scaled,
        plan_title,
        element + element_int_scaled as element_plus_element_int_scaled
    from valid
    where plan in ('pro', 'enterprise')
),
base_price_tiers as (
    select *, case when element >= 86 then 'High' when element >= 40 then 'Medium' else 'Low' end as element_tier
    from base
    where plan not in ('pro', 'basic')
),
with_windows as (
    select
        plan,
        element,
        element_int,
        min(element_int_scaled) over (partition by plan) as plan_min_element_int_scaled,
        avg(element_int) over (partition by plan) as plan_avg_element_int,
        stddev_samp(element_plus_element_int_scaled) over (partition by plan) as plan_stddev_samp_element_plus_element_int_scaled,
        stddev_samp(element_int) over (partition by plan) as plan_stddev_samp_element_int,
        stddev_samp(element) over (partition by plan) as plan_stddev_samp_element
    from base_price_tiers
    where element_int_scaled >= 62
),
grouped as (
    select
        case when plan_stddev_samp_element >= 62 then 'LARGE' else 'SMALL' end as plan_stddev_samp_element_tier,
        approx_percentile(plan_stddev_samp_element_plus_element_int_scaled, 0.5) as plan_stddev_samp_element_plus_element_int_scaled_p50,
        count(*) filter (where plan = 'pro') as pro_rows,
        count(*) as num_rows
    from with_windows
    group by case when plan_stddev_samp_element >= 62 then 'LARGE' else 'SMALL' end
)
select
    plan_stddev_samp_element_tier,
    plan_stddev_samp_element_plus_element_int_scaled_p50,
    pro_rows
from grouped
order by plan_stddev_samp_element_plus_element_int_scaled_p50 asc nulls last
