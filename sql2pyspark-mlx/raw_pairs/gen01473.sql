with base as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        round(datediff(start_date, end_date) / 30, 1) as months_end_date_to_start_date,
        concat_ws('-', plan, status) as plan_status_key
    from subscriptions
    where start_date > date '2023-10-05'
      and plan in ('enterprise', 'basic')
),
base_subscriptions as (
    -- Basic cleansing and defaults
    select
        id,
        customer_id,
        plan,
        start_date,
        end_date,
        monthly_fee,
        dayofweek(start_date) as start_date_dayofweek
    from base
    where end_date > date '2023-04-14'
      and customer_id is not null
),
with_window as (
    -- Non-equi join on a validity window
    select
        s.*,
        sub.id as subscription_id,
        sub.plan as subscription_plan,
        sub.monthly_fee as subscription_monthly_fee
    from base_subscriptions s
    left join subscriptions sub
        on s.customer_id = sub.customer_id
       and s.end_date >= sub.start_date
       and s.end_date < sub.end_date + interval 7 days
),
matched_keys as (
    -- Set intersection on keys
    select plan, id
    from with_window
    intersect
    select plan, id
    from subscriptions
    where plan = 'premium'
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from with_window s
    join matched_keys k on s.plan = k.plan and s.id = k.id
),
joined as (
    -- Attach customers attributes
    select
        s.id,
        s.customer_id,
        s.start_date,
        s.start_date_dayofweek,
        s.subscription_id,
        s.subscription_plan,
        b.signup_date
    from kept_rows s
    join customers b
        on s.customer_id = b.id
),
joined_subscriptions as (
    -- Attach customers attributes
    select
        e.id,
        e.customer_id,
        e.start_date_dayofweek,
        b.email,
        b.age
    from joined e
    left join customers b
        on e.customer_id = b.id
    where b.name like 'Oli%'
),
filtered as (
    -- Basic cleansing and defaults
    select
        *,
        coalesce(email, 'N/A') as email_filled,
        length(email) as email_length
    from joined_subscriptions
    where customer_id is not null
)
-- Grouped metrics
select
    customer_id,
    percentile_approx(start_date_dayofweek, 0.5) as start_date_dayofweek_p50,
    percentile_approx(age, array(0.25, 0.5, 0.75)) as age_quartiles,
    count(*) as row_count
from filtered
group by customer_id
having start_date_dayofweek_p50 < 1000000
order by customer_id desc;
