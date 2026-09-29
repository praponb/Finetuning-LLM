with lines as (
    -- Unnest the array into one row per element
    select
        id,
        customer_id,
        created_at,
        code
    from carts
    lateral view explode(coupon_codes) e as code
),
joined as (
    -- Attach customers attributes
    select s.*, p.email
    from lines s
    left join customers p
        on s.customer_id = p.id
    where p.country = 'TH'
),
base as (
    -- Basic cleansing and defaults
    select
        *,
        date_trunc('month', created_at) as created_at_period,
        weekofyear(created_at) as created_at_weekofyear
    from joined
    where created_at > to_timestamp('2025-03-02 02:00:00')
      and email like '%@outlook.com'
),
with_windows as (
    -- Running and moving aggregates
    select
        customer_id,
        email,
        created_at_weekofyear,
        avg(created_at_weekofyear) over (partition by customer_id order by cast(created_at as bigint) range between 7200 preceding and current row) as created_at_weekofyear_avg_2h,
        max(created_at_weekofyear) over (partition by to_date(created_at_period), code) as created_at_period_to_date_max_created_at_weekofyear
    from base
),
report as (
    select
        customer_id,
        email,
        created_at_weekofyear,
        created_at_weekofyear_avg_2h,
        created_at_period_to_date_max_created_at_weekofyear
    from with_windows
    order by created_at_weekofyear_avg_2h
)
select
    s.customer_id,
    s.email,
    s.created_at_weekofyear,
    s.created_at_weekofyear_avg_2h,
    s.created_at_period_to_date_max_created_at_weekofyear,
    (select max(t2.monthly_fee) from subscriptions t2 where t2.customer_id = s.customer_id) as max_subscription_monthly_fee
from report s
order by created_at_weekofyear desc nulls first;
