with valid as (
    select
        id,
        customer_id,
        device,
        browser,
        started_at,
        country,
        hour(started_at) as started_at_hour,
        trim(country) as clean_country
    from sessions
    where country is not null
),
with_last as (
    select
        s.*,
        x.web_event_duration_sec,
        x.web_event_page
    from valid s
    join lateral (
            select c.duration_sec as web_event_duration_sec, c.page as web_event_page
            from web_events c
            where c.session_id = s.id
            order by c.duration_sec desc, c.id desc
            limit 1
        ) x on true
),
kept as (
    select s.*
    from with_last s
    where exists (
            select 1
            from web_events ch
            where ch.session_id = s.id
              and ch.event_time < s.started_at
        )
),
scored as (
    select
        id,
        device,
        started_at,
        country,
        started_at_hour,
        web_event_duration_sec,
        last_value(country) ignore nulls over (order by web_event_duration_sec, id rows between unbounded preceding and current row) as last_known_country,
        sum(started_at_hour) over w_part as cumulative_started_at_hour
    from kept
    window w_part as (partition by web_event_page order by started_at, id rows between unbounded preceding and current row)
),
kept_sessions as (
    select s.*
    from scored s
    where not exists (
            select 1
            from web_events x
            where x.session_id = s.id
              and x.event_time >= s.started_at
        )
)
select *
from (
    select
        device,
        country,
        started_at_hour
    from kept_sessions
) src
pivot (
    sum(started_at_hour)
    for country in ('JP', 'TH', 'DE')
);
