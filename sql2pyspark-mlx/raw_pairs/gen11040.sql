with with_dims as (
    -- Attach sessions attributes
    select e.*, s.country
    from web_events e
    join sessions s
        on e.session_id = s.id
),
ranked as (
    -- Keep one row per key (latest first)
    select *
    from (
        select *, row_number() over (partition by session_id order by event_time desc nulls last, id) as rank_in_group
        from with_dims
    ) ranked
    where rank_in_group = 1
),
joined as (
    -- Attach sessions attributes
    select
        s.id,
        s.session_id,
        s.event_type,
        s.event_time,
        s.duration_sec,
        s.country,
        d.device
    from ranked s
    left join sessions d
        on s.session_id = d.id
    where d.browser = 'Firefox'
),
flagged as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when lag(event_time) over (partition by device order by event_time, id) is null
                 or event_time > lag(event_time) over (partition by device order by event_time, id) + interval 60 days then 1 else 0 end as is_new_session
    from joined
),
sessions_web_events as (
    -- Running sum of the flag = session number
    select *, sum(is_new_session) over (partition by device order by event_time, id rows between unbounded preceding and current row) as session_no
    from flagged
),
latest as (
    -- QUALIFY-style window filter
    select
        id,
        session_id,
        event_type,
        event_time,
        duration_sec,
        country,
        is_new_session,
        session_no
    from sessions_web_events
    qualify rank() over (partition by country order by is_new_session desc nulls last) = 1
)
select
    s.id,
    s.session_id,
    s.event_type,
    s.event_time,
    s.duration_sec,
    s.country,
    s.is_new_session,
    s.session_no,
    case when country rlike '^(SG|UK)$' then 'OTHER_ZONE' else 'EXTENDED' end as country_zone,
    (select max(x.duration_sec) from web_events x where x.session_id = s.session_id) as max_web_event_duration_sec
from latest s
order by id desc nulls last, session_no desc
limit 5
