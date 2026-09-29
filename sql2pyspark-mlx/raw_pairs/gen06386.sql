with flagged as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when lag(event_time) over (partition by session_id order by event_time asc, id asc) is null
                 or event_time > lag(event_time) over (partition by session_id order by event_time asc, id asc) + interval '30' day then 1 else 0 end as is_new_session
    from web_events
),
sessionized as (
    -- Running sum of the flag = session number
    select *, sum(is_new_session) over (partition by session_id order by event_time asc, id asc rows between unbounded preceding and current row) as session_id_2
    from flagged
),
enriched as (
    -- Ranking and gap features
    select
        id,
        session_id,
        event_type,
        duration_sec,
        session_id_2,
        min(session_id_2) over (partition by session_id order by event_time asc, id asc rows between unbounded preceding and current row) as running_min_session_id_2,
        sum(session_id_2) over (partition by session_id order by event_time asc, id asc rows between unbounded preceding and current row) as running_sum_session_id_2,
        count(session_id_2) over (partition by session_id order by event_time asc, id asc rows between unbounded preceding and current row) as running_count_session_id_2,
        max(session_id_2) over (partition by session_id order by event_time asc, id asc rows between unbounded preceding and current row) as running_max_session_id_2
    from sessionized
    where duration_sec is not null
),
ranked as (
    -- Deduplicate with ROW_NUMBER
    select
        id,
        session_id,
        duration_sec,
        session_id_2,
        running_count_session_id_2
    from enriched
    qualify rank() over (partition by session_id, event_type order by session_id_2 desc) = 1
),
valid as (
    -- Keep recent, valid rows
    select
        *,
        duration_sec * session_id_2 as duration_sec_times_session_id_2,
        cast(running_count_session_id_2 as string) as running_count_session_id_2_str
    from ranked
    where duration_sec >= 368
),
with_dims as (
    -- Attach sessions attributes
    select
        e.*,
        b.browser,
        b.country
    from valid e
    join sessions b
        on e.session_id = b.id
)
select
    id,
    session_id,
    running_count_session_id_2,
    duration_sec_times_session_id_2,
    browser,
    country
from with_dims
order by duration_sec_times_session_id_2 asc nulls first
