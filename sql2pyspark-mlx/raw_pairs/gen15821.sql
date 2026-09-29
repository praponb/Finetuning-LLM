with kept as (
    -- Semi-join via a correlated subquery
    select s.*
    from sessions s
    where exists (
            select 1
            from web_events ch
            where ch.session_id = s.id
              and ch.event_time < s.started_at
        )
),
flagged as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when lag(started_at) over (partition by country order by started_at asc, id asc) is null
                 or started_at > lag(started_at) over (partition by country order by started_at asc, id asc) + interval '14' day then 1 else 0 end as is_new_burst
    from kept
),
sessions_sessions as (
    -- Running sum of the flag = session number
    select *, sum(is_new_burst) over (partition by country order by started_at asc, id asc rows between unbounded preceding and current row) as burst_id
    from flagged
),
matched as (
    -- Non-equi join on a validity window
    select
        a.*,
        sub.id as subscription_id,
        sub.plan,
        sub.monthly_fee
    from sessions_sessions a
    left join subscriptions sub
        on a.customer_id = sub.customer_id
       and a.started_at between sub.start_date and sub.end_date
),
metrics as (
    -- Ranking and gap features
    select
        id,
        customer_id,
        device,
        browser,
        started_at,
        country,
        is_new_burst,
        subscription_id,
        plan,
        monthly_fee,
        lag(is_new_burst, 1) over (partition by plan order by subscription_id asc, id asc, subscription_id asc) as prev_is_new_burst,
        last_value(country) ignore nulls over (partition by country order by started_at asc, id asc, subscription_id asc rows between unbounded preceding and current row) as last_known_country
    from matched
),
with_dims as (
    -- Attach customers attributes
    select
        e.id,
        e.customer_id,
        e.browser,
        e.started_at,
        e.is_new_burst,
        e.subscription_id,
        e.plan,
        e.prev_is_new_burst,
        d.credit_limit,
        d.segment,
        d.age
    from metrics e
    join customers d
        on e.customer_id = d.id
    where d.segment not in ('Consumer', 'SMB')
),
joined as (
    -- Attach customers attributes
    select
        src.id,
        src.customer_id,
        src.started_at,
        src.subscription_id,
        src.prev_is_new_burst,
        src.credit_limit,
        src.age,
        d.email
    from with_dims src
    left join customers d
        on src.customer_id = d.id
)
-- Grouped metrics
select
    case when credit_limit >= 17700 then 'PREMIUM' else 'STANDARD' end as credit_limit_tier,
    max(started_at) as ended_at,
    (unix_timestamp(max(started_at)) - unix_timestamp(min(started_at))) / 60 as duration_min,
    min(started_at) as started_at
from joined
group by case when credit_limit >= 17700 then 'PREMIUM' else 'STANDARD' end
having duration_min > 0
order by credit_limit_tier desc
