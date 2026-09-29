with unpacked as (
    -- Higher-order array functions
    select
        id,
        service,
        log_ts,
        status_code,
        payload_json,
        coalesce(context['region'], 'unknown') as region_or_default,
        from_json(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').duration as duration_sec
    from app_logs
    where status_code > 330
)
-- Running and moving aggregates
select
    id,
    service,
    last_value(duration_sec) ignore nulls over w_ordered last_known_duration_sec,
    avg(duration_sec) over w_part duration_sec_avg_5,
    sum(status_code) over w_ordered cumulative_status_code,
    lead(duration_sec, 1) over w_part_2 next_duration_sec,
    avg(duration_sec) over w_hist duration_sec_avg_6
from unpacked
window w_ordered as (partition by service order by status_code, id rows between unbounded preceding and current row), w_part as (partition by month(log_ts) order by status_code, id rows between 5 preceding and current row), w_part_2 as (partition by service order by status_code, id), w_hist as (partition by service order by log_ts, id rows between 6 preceding and current row);
