with gaps as (
    select *, case when reading_ts - lag(reading_ts) over (partition by device_code order by reading_ts, id) > interval 45 minutes then 1 else 0 end as is_new_session
    from device_readings
),
islands as (
    select *, sum(is_new_session) over (partition by device_code order by reading_ts, id rows between unbounded preceding and current row) as session_no
    from gaps
),
latest as (
    select *, row_number() over (partition by device_code order by pressure nulls first, id) as rn
    from islands
    qualify rn = 1
),
scored as (
    select
        id,
        device_code,
        reading_ts,
        humidity,
        status_bits,
        is_new_session,
        lead(session_no, 2, 0) over (order by reading_ts, id) as next_session_no,
        avg(humidity) over (partition by month(reading_ts), device_code order by reading_ts, id rows between 3 preceding and current row) as humidity_avg_3
    from latest
),
ranked as (
    select
        id,
        reading_ts,
        next_session_no
    from scored
    qualify row_number() over (partition by device_code order by next_session_no desc nulls first, id) = 1
),
enriched as (
    select
        *,
        rank() over (order by next_session_no desc nulls first) as next_session_no_rank,
        cume_dist() over (order by next_session_no desc) as next_session_no_cume_dist
    from ranked
),
cleaned as (
    select
        *,
        date_trunc('YEAR', reading_ts) as reading_ts_period,
        hour(reading_ts) as reading_ts_hour
    from enriched
    where reading_ts > date '2025-09-01'
      and next_session_no_cume_dist > 0.79
)
select
    next_session_no,
    min(reading_ts) as first_reading_ts,
    approx_percentile(reading_ts_hour, array(0.25, 0.5, 0.75)) as reading_ts_hour_quartiles,
    count(*) as row_count,
    max(reading_ts_hour) as max_reading_ts_hour,
    round(avg(next_session_no_rank), 2) as avg_next_session_no_rank,
    max(reading_ts) as last_reading_ts
from cleaned
group by next_session_no
having avg_next_session_no_rank >= 0
order by next_session_no nulls first
