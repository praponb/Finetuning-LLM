with cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        customer_id,
        plan,
        start_date,
        end_date,
        status,
        concat(status, ' - ', plan) as status_plan
    from subscriptions
    where customer_id is not null
      and plan = 'premium'
),
kept_keys as (
    -- Set intersection on keys
    select status, customer_id
    from cleaned
    intersect
    select status, customer_id
    from subscriptions
    where customer_id is not null
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from cleaned s
    inner join kept_keys k on s.status = k.status and s.customer_id = k.customer_id
),
with_dims as (
    -- Attach customers attributes
    select a.*, c.country
    from verified a
    left join customers c
        on a.customer_id = c.id
),
cleaned_subscriptions as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        customer_id,
        start_date,
        status,
        add_months(end_date, 12) as end_date_plus_months,
        case when plan in ('basic', 'premium') then 'Priority' else 'Other' end as plan_group,
        coalesce(country, 'N/A') as country_filled
    from with_dims
    where plan in ('basic', 'premium')
),
with_dims_subscriptions as (
    -- Attach customers attributes
    select
        e.customer_id,
        e.status,
        e.end_date_plus_months,
        e.plan_group,
        p.country
    from cleaned_subscriptions e
    left join customers p
        on e.customer_id = p.id
),
with_dims_subscriptions_2 as (
    -- Attach customers attributes
    select s.*, b.segment
    from with_dims_subscriptions s
    inner join customers b
        on s.customer_id = b.id
    where b.city in ('Tokyo', 'Berlin')
)
-- Aggregate with conditional metrics
select
    date_trunc('quarter', end_date_plus_months) as quarter_start,
    size(collect_set(status)) as num_status,
    count_if(segment = 'Corporate') as corporate_events,
    count(*) as cnt,
    count_if(status = 'CANCELLED') as cancelled_events
from with_dims_subscriptions_2
group by quarter_start
