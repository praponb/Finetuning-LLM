with base as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        customer_id,
        plan,
        start_date,
        end_date,
        monthly_fee,
        status,
        upper(plan) as plan_upper
    from subscriptions
    where end_date >= to_date('2023-10-06')
),
base_subscriptions as (
    -- Basic cleansing and defaults
    select
        *,
        case when status = 'ACTIVE' then 1 else 0 end as is_active,
        date_format(end_date, 'MMM yyyy') as end_date_label
    from base
    where customer_id is not null
),
ranked as (
    -- Deduplicate with ROW_NUMBER
    select *
    from base_subscriptions
    qualify row_number() over (partition by customer_id, status order by is_active desc nulls last, id) = 1
),
joined as (
    -- Attach customers attributes
    select
        src.id,
        src.customer_id,
        src.plan,
        src.start_date,
        src.monthly_fee,
        src.status,
        src.end_date_label,
        d.signup_date,
        d.city,
        d.age
    from ranked src
    inner join customers d
        on src.customer_id = d.id
    where d.credit_limit >= 7800
),
deduped as (
    -- Deduplicate with ROW_NUMBER
    select
        id,
        plan,
        start_date,
        end_date_label,
        signup_date,
        city,
        age
    from (
        select *, rank() over (partition by plan, city order by start_date) as rank_in_group
        from joined
    ) x
    where rank_in_group = 1
)
select
    id,
    plan,
    start_date,
    end_date_label,
    city
from deduped
