with with_last as (
    select s.*, x.page
    from sessions s
    left join lateral (
            select c.page
            from web_events c
            where c.session_id = s.id
              and c.event_time <= s.started_at
            order by c.event_time desc, c.id desc
            limit 1
        ) x on true
),
gaps as (
    select *, case when started_at - lag(started_at) over (partition by customer_id order by started_at, id) > interval '60' day then 1 else 0 end as new_group_flag
    from with_last
),
sessions_sessions as (
    select *, sum(new_group_flag) over (partition by customer_id order by started_at, id rows between unbounded preceding and current row) as session_id
    from gaps
),
flagged as (
    select *, case when started_at - lag(started_at) over (partition by country order by started_at, id) > interval 60 days then 1 else 0 end as is_new_burst
    from sessions_sessions
),
sessions_sessions_2 as (
    select *, sum(is_new_burst) over (partition by country order by started_at, id rows between unbounded preceding and current row) as session_id_2
    from flagged
),
attributed as (
    select
        s.*,
        sub.id as subscription_id,
        sub.plan,
        sub.monthly_fee
    from sessions_sessions_2 s
    left join subscriptions sub
        on s.customer_id = sub.customer_id
       and s.started_at >= sub.start_date
       and s.started_at < sub.end_date + interval '7' day
),
joined as (
    select a.*, c.city
    from attributed a
    left join customers c
        on a.customer_id = c.id
),
enriched as (
    select
        id,
        customer_id,
        device,
        browser,
        started_at,
        country,
        page,
        new_group_flag,
        session_id,
        is_new_burst,
        session_id_2,
        subscription_id,
        plan,
        monthly_fee,
        city,
        rank() over (partition by country order by session_id_2 desc) as session_id_2_rank,
        ntile(5) over (partition by country order by subscription_id desc, id, subscription_id) as subscription_id_bucket
    from joined
)
select
    date_trunc('week', started_at) as week_start,
    date_trunc('year', started_at) as year_start,
    case when session_id >= 7 then 'PREMIUM' else 'STANDARD' end as session_id_tier,
    size(collect_set(country)) as num_countries,
    max(started_at) as ended_at,
    grouping(date_trunc('week', started_at)) as grouping_level
from enriched
group by grouping sets ((date_trunc('week', started_at), date_trunc('year', started_at), case when session_id >= 7 then 'PREMIUM' else 'STANDARD' end), (date_trunc('week', started_at), date_trunc('year', started_at)))
order by year_start desc;
