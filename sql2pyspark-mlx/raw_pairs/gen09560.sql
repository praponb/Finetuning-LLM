with enriched as (
    select
        id,
        customer_id,
        browser,
        started_at,
        row_number() over (partition by month(started_at), device order by started_at asc, id asc) as event_seq,
        row_number() over (partition by browser order by started_at desc, id asc) as event_seq_2,
        last_value(browser, true) over (partition by customer_id order by started_at asc, id asc rows between unbounded preceding and current row) as last_known_browser
    from sessions
),
base as (
    select
        id,
        customer_id,
        browser,
        started_at,
        event_seq,
        event_seq_2,
        round(event_seq_2 / event_seq, 2) as event_seq_2_per_event_seq,
        to_date(started_at) as started_at_date,
        coalesce(browser, 'DIRECT') as browser_clean
    from enriched
    where event_seq > 3
),
eligible as (
    select s.*
    from base s
    where not exists (
            select 1
            from web_events ch
            where ch.session_id = s.id
              and ch.event_time >= s.started_at
        )
),
not_flagged as (
    select s.*
    from eligible s
    where not exists (
            select 1
            from web_events ch
            where ch.session_id = s.id
              and ch.event_time < s.started_at
        )
),
matched as (
    select
        a.*,
        sub.id as subscription_id,
        sub.plan,
        sub.monthly_fee
    from not_flagged a
    left join subscriptions sub
        on a.customer_id = sub.customer_id
       and a.started_at_date >= sub.start_date
       and a.started_at_date < sub.end_date + interval 14 days
),
flagged as (
    select *, case when started_at - lag(started_at) over (partition by browser order by started_at asc, id asc, subscription_id asc) > interval '60' day then 1 else 0 end as new_group_flag
    from matched
),
islands as (
    select *, sum(new_group_flag) over (partition by browser order by started_at asc, id asc, subscription_id asc rows between unbounded preceding and current row) as island_id
    from flagged
),
metrics as (
    select
        *,
        row_number() over (order by event_seq_2 desc, id asc, subscription_id asc) as event_seq_3,
        cume_dist() over (partition by browser order by subscription_id desc) as subscription_id_cume_dist
    from islands
    where plan in ('standard', 'premium', 'basic')
)
select
    c.city,
    a.browser,
    count(*) as num_rows,
    min(a.event_seq_2_per_event_seq) as min_event_seq_2_per_event_seq,
    grouping_id() as level_id
from metrics a
inner join customers c on a.customer_id = c.id
group by grouping sets ((c.city, a.browser), (c.city))
order by num_rows desc;
