with exploded as (
    select
        id,
        host,
        log_ts,
        level,
        status_code,
        key,
        value
    from app_logs
    lateral view explode(context) e as key, value
),
parsed as (
    select
        id,
        log_ts,
        status_code,
        key,
        value,
        shiftright(status_code, 2) as status_code_high,
        cast((status_code & 1) != 0 as int) as flag_bit1
    from exploded
),
metrics as (
    select
        *,
        lag(flag_bit1) over (partition by key order by log_ts, id, key) as prev_flag_bit1,
        lead(flag_bit1) over (partition by key order by log_ts, id, key) as next_flag_bit1,
        lead(status_code) over (partition by key order by log_ts, id, key) as next_status_code,
        lag(status_code) over (partition by key order by log_ts, id, key) as prev_status_code,
        row_number() over (partition by key order by log_ts, id, key) as row_seq
    from parsed
),
with_windows as (
    select
        *,
        first_value(row_seq) ignore nulls over (partition by key order by status_code_high, id, key rows between unbounded preceding and current row) as first_known_row_seq,
        count(next_status_code) over (partition by key) as key_count_next_status_code,
        row_number() over (order by next_flag_bit1 desc, id, key) as event_seq
    from metrics
),
with_windows_app_logs as (
    select
        *,
        first_value(flag_bit1, true) over w_hist as first_known_flag_bit1,
        sum(status_code) over (partition by key order by status_code, id, key rows between unbounded preceding and current row) as cumulative_status_code,
        avg(next_status_code) over (partition by key order by prev_flag_bit1, id, key rows between 2 preceding and 1 preceding) as next_status_code_avg_2_prior
    from with_windows
    window w_hist as (partition by key order by key_count_next_status_code, id, key rows between unbounded preceding and current row)
)
select
    key,
    date_trunc('day', log_ts) as activity_day,
    date_trunc('hour', log_ts) as hour_window,
    percentile_approx(status_code_high, array(0.25, 0.5, 0.75)) as status_code_high_quartiles,
    size(collect_set(key)) as num_keys,
    count_if(key = 'version') as version_events,
    approx_percentile(status_code, array(0.25, 0.5, 0.75)) as status_code_quartiles,
    min(log_ts) as first_log_ts,
    bit_or(status_code) as status_code_bit_or
from with_windows_app_logs
group by 1, 2, 3
order by status_code_bit_or
