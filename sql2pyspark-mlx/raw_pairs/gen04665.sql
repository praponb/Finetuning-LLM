with with_gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when paid_at - lag(paid_at) over (partition by status order by paid_at, id) > interval 14 days then 1 else 0 end as session_start_flag
    from payments
),
sessionized as (
    -- Running sum of the flag = session number
    select *, sum(session_start_flag) over (partition by status order by paid_at, id rows between unbounded preceding and current row) as burst_id
    from with_gaps
),
valid as (
    -- Keep recent, valid rows
    select
        id,
        order_id,
        method,
        amount,
        status,
        session_start_flag,
        burst_id,
        least(amount, burst_id) as least_amount_burst_id
    from sessionized
    where burst_id >= 4
),
with_windows as (
    -- Window metrics over several different partitions
    select
        id,
        method,
        amount,
        session_start_flag,
        burst_id,
        least_amount_burst_id,
        sum(burst_id) over (partition by status order by burst_id, id rows between unbounded preceding and current row) as cumulative_burst_id,
        avg(burst_id) over w_hist as burst_id_avg_3_prior
    from valid
    where order_id in (3, 17, 18, 29)
    window w_hist as (partition by method order by session_start_flag, id rows between 3 preceding and 1 preceding)
),
scored as (
    -- Running and moving aggregates
    select
        *,
        lag(least_amount_burst_id, 1) over (partition by method order by burst_id_avg_3_prior, id) as prev_least_amount_burst_id,
        dense_rank() over (partition by method order by burst_id_avg_3_prior nulls first) as burst_id_avg_3_prior_dense_rank
    from with_windows
),
base as (
    -- Filter the raw rows and derive helper columns
    select *, floor(burst_id) as burst_id_whole
    from scored
    where method is not null
      and least_amount_burst_id < 44
)
select
    id,
    amount,
    session_start_flag,
    burst_id_avg_3_prior,
    prev_least_amount_burst_id,
    burst_id_avg_3_prior_dense_rank,
    burst_id_whole
from base
order by amount desc nulls last, prev_least_amount_burst_id;
