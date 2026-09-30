with base as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        account_id,
        txn_type,
        amount,
        txn_time,
        is_flagged,
        lower(txn_type) as txn_type_lower,
        hour(txn_time) as txn_time_hour
    from transactions
    where txn_time > to_date('2023-05-17')
),
flagged as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when lag(txn_time) over (partition by txn_type order by txn_time, id) is null
                 or txn_time > lag(txn_time) over (partition by txn_type order by txn_time, id) + interval 60 days then 1 else 0 end as session_start_flag
    from base
),
sessionized as (
    -- Running sum of the flag = session number
    select *, sum(session_start_flag) over (partition by txn_type order by txn_time, id rows between unbounded preceding and current row) as session_id
    from flagged
),
metrics as (
    -- Window metrics over several different partitions
    select
        id,
        amount,
        txn_time,
        is_flagged,
        txn_type_lower,
        txn_time_hour,
        session_start_flag,
        sum(txn_time_hour) over (partition by date_trunc('WEEK', txn_time), account_id order by cast(txn_time as bigint) range between 7776000 preceding and current row) as txn_time_hour_sum_90d,
        rank() over (partition by account_id, txn_type order by session_start_flag nulls first) as session_start_flag_rank,
        sum(txn_time_hour) over (partition by year(txn_time), txn_type order by session_id, id rows between unbounded preceding and current row) as cumulative_txn_time_hour,
        row_number() over (partition by account_id, txn_type order by txn_time, id) as seq_no
    from sessionized
),
scored as (
    -- Window metrics over several different partitions
    select
        id,
        amount,
        txn_time,
        txn_type_lower,
        txn_time_hour_sum_90d,
        seq_no,
        stddev_samp(seq_no) over (partition by date_trunc('month', txn_time)) as txn_time_period_stddev_samp_seq_no,
        ntile(4) over (order by amount, id) as amount_bucket,
        sum(session_start_flag_rank) over (order by amount, id rows between unbounded preceding and current row) as running_session_start_flag_rank,
        sum(txn_time_hour_sum_90d) over (order by session_start_flag_rank, id rows between unbounded preceding and current row) as running_txn_time_hour_sum_90d
    from metrics
),
scored_transactions as (
    -- Running and moving aggregates
    select
        *,
        count(seq_no) over (order by amount, id rows between unbounded preceding and current row) as running_count_seq_no,
        max(seq_no) over (order by amount, id rows between unbounded preceding and current row) as running_max_seq_no,
        min(seq_no) over (order by amount, id rows between unbounded preceding and current row) as running_min_seq_no,
        sum(seq_no) over (order by amount, id rows between unbounded preceding and current row) as running_sum_seq_no,
        first_value(running_txn_time_hour_sum_90d, true) over (order by amount_bucket, id rows between unbounded preceding and current row) as first_known_running_txn_time_hour_sum_90d
    from scored
    where txn_time_period_stddev_samp_seq_no < 5
)
-- Aggregate with conditional metrics
select
    date_trunc('year', txn_time) as year_start,
    case when running_min_seq_no >= 7.3 then 'HIGH' else 'LOW' end as running_min_seq_no_tier,
    date_trunc('quarter', txn_time) as quarter_start,
    min(running_sum_seq_no) as min_running_sum_seq_no,
    approx_percentile(txn_time_hour_sum_90d, array(0.25, 0.5, 0.75)) as txn_time_hour_sum_90d_quartiles,
    percent_rank() over (partition by date_trunc('year', txn_time) order by min(running_sum_seq_no) desc) as min_running_sum_seq_no_percent_rank
from scored_transactions
group by date_trunc('year', txn_time), case when running_min_seq_no >= 7.3 then 'HIGH' else 'LOW' end, date_trunc('quarter', txn_time);
