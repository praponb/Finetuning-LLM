with matched_keys as (
    -- Set intersection on keys
    select customer_id
    from subscriptions
    intersect all
    select customer_id
    from subscriptions
    where month(end_date) in (3, 9, 11)
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from subscriptions s
    join matched_keys k on s.customer_id = k.customer_id
),
with_dims as (
    -- Attach customers attributes
    select
        e.*,
        c.city,
        c.name
    from reconciled e
    join customers c
        on e.customer_id = c.id
),
with_dims_subscriptions as (
    -- Attach customers attributes
    select
        src.*,
        b.signup_date,
        b.email,
        b.city as customer_city
    from with_dims src
    join customers b
        on src.customer_id = b.id
),
remaining_keys as (
    -- Set intersection on keys
    select id, plan
    from with_dims_subscriptions
    intersect
    select id, plan
    from subscriptions
    where start_date between '2023-12-01' and '2025-01-01'
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from with_dims_subscriptions s
    join remaining_keys k on s.id = k.id and s.plan = k.plan
),
stats as (
    -- Aggregate with conditional metrics
    select
        status,
        count(*) as cnt,
        count(*) as cnt_2,
        count(*) filter (where customer_city = 'Singapore') as singapore_rows
    from kept_rows
    group by 1
    having cnt_2 < 1
)
select
    status,
    cnt_2,
    singapore_rows,
    case when status rlike '^(CANCELLED|EXPIRED)$' then 'OTHER_ZONE' when status rlike '^(ACTIVE|PAUSED)$' then 'EXTENDED' else 'GROUP_A' end as status_zone
from stats
order by singapore_rows nulls first
