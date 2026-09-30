with lines as (
    -- Unnest the array into one row per element
    select
        id,
        service,
        log_ts,
        latency_ms,
        status_code,
        k,
        attr_value
    from app_logs
    lateral view explode(context) li as k, attr_value
),
parsed as (
    -- Semi-structured parsing
    select
        id,
        service,
        log_ts,
        k,
        shiftright(status_code, 2) as status_code_high,
        cast((status_code & 2) != 0 as int) as flag_bit2
    from lines
    where service != 'search'
),
enriched as (
    -- Ranking and gap features
    select
        id,
        service,
        log_ts,
        k,
        status_code_high,
        first_value(flag_bit2, true) over (partition by to_date(log_ts) order by status_code_high, id, k rows between unbounded preceding and current row) first_known_flag_bit2,
        ntile(3) over (partition by service order by status_code_high, id, k) status_code_high_bucket,
        avg(status_code_high) over (partition by to_date(log_ts), k order by cast(log_ts as bigint) range between 7200 preceding and current row) status_code_high_avg_2h,
        percent_rank() over w_recent flag_bit2_percent_rank
    from parsed
    window w_recent as (partition by k order by flag_bit2 desc nulls first)
),
with_windows as (
    -- Running and moving aggregates
    select
        *,
        avg(status_code_high_avg_2h) over (partition by k, service order by status_code_high_avg_2h, id, k rows between 3 preceding and 1 preceding) status_code_high_avg_2h_avg_3_prior,
        row_number() over (partition by k order by first_known_flag_bit2 desc, id, k) row_seq,
        ntile(5) over (partition by service order by status_code_high desc, id, k) status_code_high_bucket_2,
        sum(status_code_high_avg_2h) over (order by first_known_flag_bit2, id, k rows between unbounded preceding and current row) cumulative_status_code_high_avg_2h,
        last_value(status_code_high, true) over (order by log_ts, id, k rows between unbounded preceding and current row) last_known_status_code_high
    from enriched
),
gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when lag(log_ts) over (partition by k order by log_ts, id, k) is null
                 or log_ts > lag(log_ts) over (partition by k order by log_ts, id, k) + interval '20' minute then 1 else 0 end as session_start_flag
    from with_windows
),
islands as (
    -- Running sum of the flag = session number
    select *, sum(session_start_flag) over (partition by k order by log_ts, id, k rows between unbounded preceding and current row) as session_no
    from gaps
)
select
    id,
    log_ts,
    k,
    status_code_high,
    status_code_high_bucket,
    status_code_high_avg_2h,
    flag_bit2_percent_rank,
    row_seq,
    status_code_high_bucket_2
from islands
order by status_code_high_bucket_2 desc, flag_bit2_percent_rank desc;
