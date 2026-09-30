with valid as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        session_id,
        event_type,
        page,
        event_time,
        duration_sec,
        length(page) as page_length,
        coalesce(page, 'OTHER') as page_clean
    from web_events
    where event_time >= timestamp '2023-03-31 05:00:00'
      and page is not null
),
flagged as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when lag(event_time) over (partition by event_type order by event_time, id) is null
                 or event_time > lag(event_time) over (partition by event_type order by event_time, id) + interval '14' day then 1 else 0 end as session_start_flag
    from valid
),
islands as (
    -- Running sum of the flag = session number
    select *, sum(session_start_flag) over (partition by event_type order by event_time, id rows between unbounded preceding and current row) as island_id
    from flagged
)
-- Multi-level aggregation
select
    case when duration_sec >= 362 then 'HIGH' else 'LOW' end as duration_sec_tier,
    date_trunc('quarter', event_time) as quarter_start,
    event_type,
    (unix_timestamp(max(event_time)) - unix_timestamp(min(event_time))) / 60 as duration_min,
    percentile_approx(duration_sec, 0.9) as duration_sec_p90,
    size(collect_set(event_type)) as num_event_types,
    round(sum(session_start_flag) / nullif(count(case when event_type = 'add_to_cart' then 1 end), 0), 2) as session_start_flag_per_add_to_cart,
    count(case when page = '/home' then 1 end) as home_count
from islands
group by case when duration_sec >= 362 then 'HIGH' else 'LOW' end, date_trunc('quarter', event_time), event_type;
