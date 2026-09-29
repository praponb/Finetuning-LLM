with latest as (
    -- QUALIFY-style window filter
    select *, row_number() over (partition by page order by duration_sec desc, id) as dedup_rank
    from web_events
    qualify dedup_rank = 1
),
cleaned as (
    -- Basic cleansing and defaults
    select
        *,
        round(duration_sec, 1) as duration_sec_rounded,
        coalesce(page, 'OTHER') as page_filled
    from latest
    where page is not null
      and page is distinct from '/checkout'
),
gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when lag(event_time) over (partition by event_type order by event_time, id) is null
                 or event_time > lag(event_time) over (partition by event_type order by event_time, id) + interval '7' day then 1 else 0 end as is_new_burst
    from cleaned
),
islands as (
    -- Running sum of the flag = session number
    select *, sum(is_new_burst) over (partition by event_type order by event_time, id rows between unbounded preceding and current row) as burst_id
    from gaps
),
enriched as (
    -- Attach sessions attributes
    select
        s.id,
        s.session_id,
        s.event_type,
        s.page,
        s.event_time,
        s.duration_sec,
        s.burst_id,
        b.device
    from islands s
    left join sessions b
        on s.session_id = b.id
),
flagged as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when event_time - lag(event_time) over (partition by device order by event_time, id) > interval 60 days then 1 else 0 end as is_new_burst
    from enriched
),
sessions_web_events as (
    -- Running sum of the flag = session number
    select *, sum(is_new_burst) over (partition by device order by event_time, id rows between unbounded preceding and current row) as island_id
    from flagged
),
flagged_2 as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when event_time - lag(event_time) over (partition by page order by event_time, id) > interval '7' day then 1 else 0 end as is_new_burst_2
    from sessions_web_events
),
sessionized as (
    -- Running sum of the flag = session number
    select
        id,
        event_type,
        page,
        event_time,
        is_new_burst,
        sum(is_new_burst_2) over (partition by page order by event_time, id rows between unbounded preceding and current row) as burst_id_2
    from flagged_2
)
-- Multi-level aggregation
select
    page,
    count(distinct event_type) as distinct_event_types,
    round(sum(is_new_burst) / nullif(count(case when page = '/home' then 1 end), 0), 2) as is_new_burst_per_home,
    sort_array(collect_set(event_type)) as event_type_list,
    count(*) as row_count,
    (unix_timestamp(max(event_time)) - unix_timestamp(min(event_time))) / 60 as span_minutes
from sessionized
group by page
order by span_minutes nulls last
