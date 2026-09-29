with valid as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        email,
        country,
        city,
        segment,
        signup_date,
        is_active,
        credit_limit,
        age,
        round(credit_limit / age, 2) as credit_limit_per_age,
        coalesce(segment, 'UNKNOWN') as segment_filled
    from customers
    where signup_date > date '2023-04-05'
      and city is not null
),
cleaned as (
    -- Keep recent, valid rows
    select
        *,
        case when is_active then 'Yes' else 'No' end as is_active_label,
        coalesce(city, 'UNKNOWN') as city_clean
    from valid
    where segment is distinct from 'SMB'
),
not_flagged as (
    -- Semi-join via a correlated subquery
    select s.*
    from cleaned s
    where exists (
            select 1
            from subscriptions x
            where x.customer_id = s.id
              and x.monthly_fee between 60 and 120
        )
),
filtered as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        concat_ws('-', segment, email) as segment_email_key,
        upper(email) as email_upper
    from not_flagged
    where signup_date >= to_date('2023-06-26')
      and city is not distinct from 'Tokyo'
),
latest_match as (
    -- Latest matching subscription per row (LATERAL ... LIMIT 1)
    select
        s.id,
        s.email,
        s.city,
        s.age,
        s.credit_limit_per_age,
        s.city_clean,
        s.segment_email_key,
        s.email_upper,
        latest.subscription_plan
    from filtered s
    join lateral (
            select c.plan as subscription_plan
            from subscriptions c
            where c.customer_id = s.id
              and c.end_date <= s.signup_date
            order by c.end_date desc, c.id desc
            limit 1
        ) latest on true
)
-- Aggregate with conditional metrics
select
    subscription_plan,
    city,
    sum(credit_limit_per_age) filter (where subscription_plan = 'standard') as standard_total_credit_limit_per_age,
    round(avg(age), 2) as avg_age,
    size(collect_set(subscription_plan)) as num_subscription_plans,
    count(*) filter (where city = 'Singapore') as singapore_rows,
    bit_or(age) as age_bit_or,
    grouping(subscription_plan) as grouping_level
from latest_match
group by rollup (subscription_plan, city)
order by singapore_rows nulls last;
