with flattened as (
    -- Unnest the array into one row per element
    select
        id,
        service,
        log_ts,
        status_code,
        attr_key,
        attr_value
    from app_logs
    lateral view explode(context) x as attr_key, attr_value
),
gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when log_ts - lag(log_ts) over (partition by service order by log_ts, id, attr_key) > interval 15 minutes then 1 else 0 end as new_group_flag
    from flattened
),
sessionized as (
    -- Running sum of the flag = session number
    select *, sum(new_group_flag) over (partition by service order by log_ts, id, attr_key rows between unbounded preceding and current row) as burst_id
    from gaps
),
enriched as (
    -- Running and moving aggregates
    select
        id,
        attr_key,
        row_number() over (partition by service order by new_group_flag desc, id, attr_key) as seq_no,
        last_value(new_group_flag, true) over (partition by attr_key order by new_group_flag, id, attr_key rows between unbounded preceding and current row) as last_known_new_group_flag,
        last_value(new_group_flag, true) over (partition by service, attr_key order by burst_id, id, attr_key rows between unbounded preceding and current row) as last_known_new_group_flag_2
    from sessionized
    where burst_id >= 3
),
metrics as (
    -- Window metrics over several different partitions
    select
        *,
        sum(last_known_new_group_flag_2) over (order by last_known_new_group_flag, id, attr_key rows between unbounded preceding and current row) as cumulative_last_known_new_group_flag_2,
        row_number() over (order by last_known_new_group_flag desc, id, attr_key) as seq_no_2,
        sum(last_known_new_group_flag_2) over (partition by attr_key) as attr_key_sum_last_known_new_group_flag_2,
        avg(seq_no) over (partition by attr_key order by last_known_new_group_flag_2, id, attr_key rows between 5 preceding and current row) as seq_no_avg_5,
        sum(last_known_new_group_flag) over (partition by attr_key order by last_known_new_group_flag_2, id, attr_key rows between unbounded preceding and current row) as running_last_known_new_group_flag
    from enriched
),
with_windows as (
    -- Window metrics over several different partitions
    select
        id,
        attr_key,
        seq_no,
        last_known_new_group_flag,
        cumulative_last_known_new_group_flag_2,
        seq_no_avg_5,
        running_last_known_new_group_flag,
        lag(seq_no, 2) over w_recent as prev_seq_no,
        row_number() over (partition by attr_key order by seq_no desc, id, attr_key) as row_seq,
        first_value(last_known_new_group_flag_2) ignore nulls over w_part as first_known_last_known_new_group_flag_2,
        row_number() over w_recent as seq_no_3,
        lag(last_known_new_group_flag_2, 2) over (partition by attr_key order by seq_no_avg_5, id, attr_key) as prev_last_known_new_group_flag_2
    from metrics
    window w_recent as (partition by attr_key order by running_last_known_new_group_flag, id, attr_key), w_part as (partition by attr_key order by last_known_new_group_flag, id, attr_key rows between unbounded preceding and current row)
),
scored as (
    -- Running and moving aggregates
    select
        id,
        attr_key,
        last_known_new_group_flag,
        cumulative_last_known_new_group_flag_2,
        seq_no_avg_5,
        row_seq,
        first_known_last_known_new_group_flag_2,
        seq_no_3,
        prev_last_known_new_group_flag_2,
        lead(first_known_last_known_new_group_flag_2, 1) over (order by seq_no_avg_5, id, attr_key) as next_first_known_last_known_new_group_flag_2,
        sum(row_seq) over (partition by attr_key order by seq_no_3, id, attr_key rows between unbounded preceding and current row) as cumulative_row_seq,
        stddev_samp(row_seq) over w_grp as attr_key_stddev_samp_row_seq
    from with_windows
    window w_grp as (partition by attr_key)
),
latest as (
    -- QUALIFY-style window filter
    select *, row_number() over (partition by attr_key order by next_first_known_last_known_new_group_flag_2 desc, id, attr_key) as rank_in_group
    from scored
    qualify rank_in_group = 1
)
select
    id,
    attr_key,
    cumulative_last_known_new_group_flag_2,
    seq_no_avg_5,
    first_known_last_known_new_group_flag_2,
    seq_no_3,
    prev_last_known_new_group_flag_2,
    cumulative_row_seq,
    rank_in_group,
    case when attr_key rlike '^(version|trace)$' then 'PRIORITY' when attr_key rlike '^(region)$' then 'GROUP_A' else 'OTHER_ZONE' end as attr_key_zone
from latest
order by seq_no_3 desc, seq_no_avg_5 desc;
