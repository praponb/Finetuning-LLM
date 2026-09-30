with gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when lag(paid_at) over (partition by status order by paid_at, id) is null
                 or paid_at > lag(paid_at) over (partition by status order by paid_at, id) + interval 90 days then 1 else 0 end as new_group_flag
    from payments
),
sessions_payments as (
    -- Running sum of the flag = session number
    select
        id,
        amount,
        paid_at,
        status,
        sum(new_group_flag) over (partition by status order by paid_at, id rows between unbounded preceding and current row) as session_id
    from gaps
),
with_gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when lag(paid_at) over (partition by status order by paid_at, id) is null
                 or paid_at > lag(paid_at) over (partition by status order by paid_at, id) + interval 90 days then 1 else 0 end as new_group_flag
    from sessions_payments
),
sessions_payments_2 as (
    -- Running sum of the flag = session number
    select *, sum(new_group_flag) over (partition by status order by paid_at, id rows between unbounded preceding and current row) as session_no
    from with_gaps
),
with_windows as (
    -- Ranking and gap features
    select
        *,
        lead(session_no) over w_recent as next_session_no,
        lag(session_no) over w_recent as prev_session_no,
        lag(amount) over w_recent as prev_amount,
        row_number() over w_recent as seq_no
    from sessions_payments_2
    window w_recent as (partition by status order by session_no, id)
),
base as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        next_session_no - session_id as next_session_no_minus_session_id,
        least(seq_no, new_group_flag) as least_seq_no_new_group_flag
    from with_windows
    where prev_amount <= 1220
)
-- Multi-level aggregation
select
    date_trunc('year', paid_at) as year_start,
    date_trunc('month', paid_at) as activity_month,
    case when least_seq_no_new_group_flag >= 77 then 'PREMIUM' else 'STANDARD' end as least_seq_no_new_group_flag_tier,
    sum(session_no) as total_session_no,
    max(paid_at) as last_seen,
    sort_array(collect_set(status)) as status_list,
    size(collect_set(status)) as num_status,
    avg(prev_amount) filter (where status = 'SUCCESS') as success_avg_prev_amount,
    grouping_id() as grouping_id
from base
group by grouping sets ((date_trunc('year', paid_at), date_trunc('month', paid_at), case when least_seq_no_new_group_flag >= 77 then 'PREMIUM' else 'STANDARD' end), (date_trunc('year', paid_at), date_trunc('month', paid_at)))
having success_avg_prev_amount >= 0
order by least_seq_no_new_group_flag_tier desc
