with valid as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        customer_id,
        plan,
        start_date,
        monthly_fee,
        substring(status, 1, 4) as status_prefix
    from subscriptions
    where customer_id is not null
),
latest as (
    -- Deduplicate with ROW_NUMBER
    select
        id,
        customer_id,
        plan
    from (
        select *, row_number() over (partition by plan order by monthly_fee, id) as rn
        from valid
    ) ranked
    where rn = 1
),
base as (
    -- Filter the raw rows and derive helper columns
    select *, trim(plan) as clean_plan
    from latest
    where customer_id = 23
),
with_dims as (
    -- Attach customers attributes
    select
        e.id,
        e.customer_id,
        e.plan,
        e.clean_plan,
        d.credit_limit,
        d.age,
        d.email
    from base e
    inner join customers d
        on e.customer_id = d.id
    where d.segment = 'Consumer'
),
base_subscriptions as (
    -- Basic cleansing and defaults
    select *, cast(credit_limit as int) as credit_limit_int
    from with_dims
    where customer_id is not null
),
metrics as (
    -- Running and moving aggregates
    select
        id,
        plan,
        credit_limit,
        max(credit_limit_int) over (partition by customer_id order by age, id rows between 3 preceding and current row) as last4_max_credit_limit_int,
        min(credit_limit_int) over (partition by customer_id order by age, id rows between 3 preceding and current row) as last4_min_credit_limit_int,
        sum(credit_limit_int) over (partition by customer_id order by age, id rows between 3 preceding and current row) as last4_sum_credit_limit_int,
        ntile(10) over (partition by plan order by credit_limit_int desc, id) as credit_limit_int_bucket
    from base_subscriptions
),
scored as (
    -- Window metrics over several different partitions
    select
        id,
        credit_limit,
        last4_min_credit_limit_int,
        row_number() over (order by credit_limit, id) as event_seq,
        cume_dist() over (partition by plan order by credit_limit) as credit_limit_cume_dist
    from metrics
)
-- Native SQL UNPIVOT (wide to long)
select
    id,
    metric_name,
    value
from (
    select
        id,
        credit_limit,
        credit_limit_cume_dist,
        last4_min_credit_limit_int
    from scored
) src
unpivot (
    value for metric_name in (credit_limit as CREDIT_LIMIT, credit_limit_cume_dist as CREDIT_LIMIT_CUME_DIST_VAL, last4_min_credit_limit_int as LAST4_MIN_CREDIT_LIMIT_INT_VAL)
)
