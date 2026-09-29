with valid as (
    -- Basic cleansing and defaults
    select
        id,
        session_id,
        event_type,
        page,
        event_time,
        coalesce(duration_sec, 0) duration_sec_filled,
        coalesce(page, 'OTHER') resolved_page
    from web_events
    where page is not null
      and session_id in (17, 20, 36)
),
gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when event_time - lag(event_time) over (partition by event_type order by event_time, id) > interval '7' day then 1 else 0 end as new_group_flag
    from valid
),
islands as (
    -- Running sum of the flag = session number
    select *, sum(new_group_flag) over (partition by event_type order by event_time, id rows between unbounded preceding and current row) as session_no
    from gaps
),
latest as (
    -- Deduplicate with ROW_NUMBER
    select
        id,
        event_type,
        resolved_page,
        new_group_flag,
        session_no
    from islands
    qualify row_number() over (partition by session_id, page order by duration_sec_filled desc nulls last, id) = 1
),
enriched as (
    -- Running and moving aggregates
    select
        *,
        count(session_no) over w_hist last3_count_session_no,
        max(session_no) over w_hist last3_max_session_no,
        avg(session_no) over w_hist last3_avg_session_no,
        min(session_no) over w_hist last3_min_session_no
    from latest
    where session_no >= 7
    window w_hist as (partition by event_type order by session_no, id rows between 2 preceding and current row)
),
ranked as (
    -- QUALIFY-style window filter
    select *
    from enriched
    qualify row_number() over (partition by event_type order by last3_max_session_no desc, id) = 1
)
-- Multi-level aggregation
select
    case when last3_min_session_no >= 7.7 then 'HIGH' else 'LOW' end as last3_min_session_no_tier,
    event_type,
    session_no,
    sort_array(collect_set(event_type)) as event_type_list,
    round(sum(last3_max_session_no) / nullif(count(case when event_type = 'add_to_cart' then 1 end), 0), 2) as last3_max_session_no_per_add_to_cart,
    count(*) as num_rows,
    count(distinct id) as distinct_ids
from ranked
group by case when last3_min_session_no >= 7.7 then 'HIGH' else 'LOW' end, event_type, session_no
