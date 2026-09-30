with valid as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        date_format(event_time, 'yyyy-MM-dd') as event_time_label,
        length(event_type) as event_type_length,
        coalesce(page, 'NONE') as page_clean
    from web_events
    where page != '/cart'
      and page is not null
),
prev_events as (
    -- Previous event time per key
    select *, lag(event_time) over (partition by session_id order by event_time, id) as prev_event_time
    from valid
),
with_gaps as (
    -- Flag rows that start a new session
    select *, case when prev_event_time is null or event_time > prev_event_time + interval 90 days then 1 else 0 end as is_new_session
    from prev_events
),
islands as (
    -- Running sum of the flag = session number
    select *, sum(is_new_session) over (partition by session_id order by event_time, id rows between unbounded preceding and current row) as session_no
    from with_gaps
)
-- Multi-level aggregation
select
    case when is_new_session >= 1 then 'HIGH' else 'LOW' end as is_new_session_tier,
    session_id,
    case when session_no >= 7 then 'LARGE' else 'SMALL' end as session_no_tier,
    max(is_new_session) filter (where event_type = 'click') as click_max_is_new_session,
    count(case when page = '/home' then 1 end) as home_count,
    sum(case when page = '/search' then is_new_session else 0 end) as search_is_new_session,
    sum(case when event_type = 'search' then session_no else 0 end) as search_session_no,
    (unix_timestamp(max(prev_event_time)) - unix_timestamp(min(prev_event_time))) / 60 as duration_min,
    grouping_id() as grouping_level
from islands
group by rollup (case when is_new_session >= 1 then 'HIGH' else 'LOW' end, session_id, case when session_no >= 7 then 'LARGE' else 'SMALL' end)
having max(is_new_session) filter (where event_type = 'click') > 0;
