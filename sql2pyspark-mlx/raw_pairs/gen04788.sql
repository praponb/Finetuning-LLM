with cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        session_id,
        event_type,
        event_time,
        duration_sec,
        cast(duration_sec as double) duration_sec_num,
        substring(event_type, 1, 2) event_type_prefix,
        coalesce(page, 'N/A') resolved_page
    from web_events
    where page is not null
),
prev_events as (
    -- Previous event time per key
    select *, lag(event_time) over (partition by session_id order by event_time, id) as prev_event_time
    from cleaned
),
gaps as (
    -- Flag rows that start a new session
    select *, case when prev_event_time is null or event_time > prev_event_time + interval '90' day then 1 else 0 end as is_new_session
    from prev_events
),
sessionized as (
    -- Running sum of the flag = session number
    select *, sum(is_new_session) over (partition by session_id order by event_time, id rows between unbounded preceding and current row) as session_id_2
    from gaps
),
metrics as (
    -- Window metrics over several different partitions
    select
        *,
        max(duration_sec_num) over (partition by event_type, session_id) event_type_max_duration_sec_num,
        avg(session_id_2) over (partition by session_id order by is_new_session, id rows between 2 preceding and 1 preceding) session_id_2_avg_2_prior,
        sum(duration_sec_num) over (partition by date_trunc('WEEK', event_time), session_id order by session_id_2, id rows between unbounded preceding and current row) cumulative_duration_sec_num,
        count(duration_sec) over (partition by event_type, session_id order by cast(event_time as bigint) range between 7776000 preceding and current row) duration_sec_count_90d
    from sessionized
    where session_id_2 between 2 and 7
),
deduped as (
    -- QUALIFY-style window filter
    select
        id,
        session_id,
        duration_sec_num,
        event_type_prefix,
        resolved_page,
        is_new_session,
        cumulative_duration_sec_num
    from (
        select *, row_number() over (partition by session_id, event_type order by cumulative_duration_sec_num desc nulls first, id) as latest_rank
        from metrics
    ) t
    where latest_rank = 1
),
enriched as (
    -- Attach sessions attributes
    select
        s.*,
        b.country,
        b.device,
        b.browser
    from deduped s
    left join sessions b
        on s.session_id = b.id
)
select
    id,
    event_type_prefix,
    is_new_session,
    cumulative_duration_sec_num,
    country,
    device,
    case when country rlike '^(JP|DE)$' then 'OTHER_ZONE' else 'EXTENDED' end as country_zone
from enriched
order by country_zone desc;
