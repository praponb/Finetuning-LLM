with valid as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        method,
        amount,
        paid_at,
        status,
        trim(method) as clean_method
    from payments
    where paid_at > date '2023-10-13'
      and method is not null
),
with_gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when lag(paid_at) over (partition by status order by paid_at, id) is null
                 or paid_at > lag(paid_at) over (partition by status order by paid_at, id) + interval '14' day then 1 else 0 end as is_new_burst
    from valid
),
sessionized as (
    -- Running sum of the flag = session number
    select
        id,
        method,
        amount,
        paid_at,
        status,
        sum(is_new_burst) over (partition by status order by paid_at, id rows between unbounded preceding and current row) as session_no
    from with_gaps
),
flagged as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when paid_at - lag(paid_at) over (partition by method order by paid_at, id) > interval '14' day then 1 else 0 end as is_new_session
    from sessionized
),
sessionized_payments as (
    -- Running sum of the flag = session number
    select *, sum(is_new_session) over (partition by method order by paid_at, id rows between unbounded preceding and current row) as burst_id
    from flagged
),
base as (
    -- Basic cleansing and defaults
    select *, ceil(burst_id) as burst_id_whole
    from sessionized_payments
    where paid_at >= timestamp '2023-10-21 04:00:00'
      and method is not null
),
enriched as (
    -- Window metrics over several different partitions
    select
        id,
        method,
        amount,
        paid_at,
        session_no,
        is_new_session,
        burst_id_whole,
        lag(is_new_session) over (partition by method, status order by burst_id, id) as prev_is_new_session,
        lead(is_new_session) over (partition by method, status order by burst_id, id) as next_is_new_session,
        lag(amount) over (partition by method, status order by burst_id, id) as prev_amount,
        row_number() over (partition by method, status order by burst_id, id) as event_seq,
        avg(burst_id) over (partition by date_trunc('WEEK', paid_at), status order by burst_id, id rows between 6 preceding and current row) as burst_id_avg_6
    from base
),
metrics as (
    -- Window metrics over several different partitions
    select
        id,
        method,
        amount,
        paid_at,
        is_new_session,
        prev_is_new_session,
        next_is_new_session,
        prev_amount,
        burst_id_avg_6,
        first_value(burst_id_whole, true) over w_part as first_known_burst_id_whole,
        avg(prev_amount) over (partition by method order by burst_id_whole, id rows between 5 preceding and current row) as prev_amount_avg_5,
        row_number() over (order by amount, id) as row_seq
    from enriched
    where burst_id_avg_6 != 7
    window w_part as (partition by method order by next_is_new_session, id rows between unbounded preceding and current row)
)
-- Grouped metrics
select
    method,
    count_if(method = 'cash') as cash_events,
    sort_array(collect_set(method)) as method_list,
    round(avg(prev_is_new_session), 2) as avg_prev_is_new_session,
    round(avg(burst_id_avg_6), 2) as avg_burst_id_avg_6
from metrics
group by 1
order by method desc
