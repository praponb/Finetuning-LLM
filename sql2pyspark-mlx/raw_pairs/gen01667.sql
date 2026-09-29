with lines as (
    select
        id,
        plan,
        explode(tier_prices) as tier_value
    from price_tiers
),
valid as (
    select
        *,
        cast(tier_value as int) as tier_value_int,
        lower(plan) as plan_lower
    from lines
    where plan in ('enterprise', 'basic', 'starter')
),
matched_keys as (
    select id
    from valid
    except all
    select id
    from price_tiers
    where plan in ('pro', 'basic')
),
kept_rows as (
    select s.*
    from valid s
    inner join matched_keys k on s.id = k.id
),
base as (
    select
        plan,
        tier_value,
        tier_value_int,
        plan_lower,
        trim(plan) as clean_plan
    from kept_rows
    where plan = 'pro'
),
remaining_keys as (
    select plan
    from base
    except distinct
    select plan
    from price_tiers
    where currency <> 'USD'
),
kept_rows_price_tiers as (
    select s.*
    from base s
    inner join remaining_keys k on s.plan = k.plan
),
metrics as (
    select
        plan,
        tier_value_int,
        percent_rank() over (order by tier_value_int desc) as tier_value_int_percent_rank,
        sum(tier_value) over (partition by plan) as plan_sum_tier_value,
        min(tier_value) over (partition by plan) as plan_min_tier_value,
        sum(tier_value_int) over (partition by plan) as plan_sum_tier_value_int
    from kept_rows_price_tiers
    where plan <> 'pro'
)
select
    plan,
    case when plan_sum_tier_value_int >= 326 then 'PREMIUM' else 'STANDARD' end as plan_sum_tier_value_int_tier,
    approx_percentile(tier_value_int, array(0.25, 0.5, 0.75)) as tier_value_int_quartiles,
    count(case when plan = 'enterprise' then 1 end) as enterprise_count,
    count_if(plan = 'pro') as pro_events
from metrics
group by cube (plan, case when plan_sum_tier_value_int >= 326 then 'PREMIUM' else 'STANDARD' end);
