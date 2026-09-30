with filtered as (
    -- Basic cleansing and defaults
    select
        id,
        session_id,
        page,
        event_time,
        duration_sec,
        lower(page) as page_lower,
        coalesce(page, 'DIRECT') as page_clean
    from web_events
    where to_date(event_time) < '2023-11-01'
),
with_gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when event_time - lag(event_time) over (partition by page order by event_time, id) > interval '7' day then 1 else 0 end as is_new_session
    from filtered
),
sessions_web_events as (
    -- Running sum of the flag = session number
    select
        id,
        page,
        event_time,
        duration_sec,
        page_lower,
        sum(is_new_session) over (partition by page order by event_time, id rows between unbounded preceding and current row) as session_id_2
    from with_gaps
)
-- Aggregate with conditional metrics
select
    case when session_id_2 >= 7 then 'PREMIUM' else 'STANDARD' end as session_id_2_tier,
    round(sum(duration_sec) / nullif(count(case when page = '/product' then 1 end), 0), 2) as duration_sec_per_product,
    sum(session_id_2) filter (where page = '/product') as product_total_session_id_2,
    approx_percentile(duration_sec, 0.5) as duration_sec_p50
from sessions_web_events
group by case when session_id_2 >= 7 then 'PREMIUM' else 'STANDARD' end
having duration_sec_p50 >= 0
order by duration_sec_p50 nulls last
