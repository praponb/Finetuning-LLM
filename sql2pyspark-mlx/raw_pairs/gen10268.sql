with cleaned as (
    -- Keep recent, valid rows
    select
        id,
        order_id,
        method,
        amount,
        paid_at,
        status,
        case when method = 'cash' then 1 else 0 end as is_cash,
        coalesce(method, 'DIRECT') as method_clean
    from payments
    where order_id is not null
),
flagged as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when lag(paid_at) over (partition by method order by paid_at, id) is null
                 or paid_at > lag(paid_at) over (partition by method order by paid_at, id) + interval 30 days then 1 else 0 end as is_new_session
    from cleaned
),
islands as (
    -- Running sum of the flag = session number
    select *, sum(is_new_session) over (partition by method order by paid_at, id rows between unbounded preceding and current row) as burst_id
    from flagged
),
metrics as (
    -- Running and moving aggregates
    select
        *,
        lead(amount, 2, 0) over (order by burst_id, id) as next_amount,
        row_number() over (partition by order_id order by paid_at, id) as row_seq
    from islands
    where order_id = 6
),
scored as (
    -- Ranking and gap features
    select
        id,
        order_id,
        amount,
        paid_at,
        status,
        is_new_session,
        dense_rank() over (partition by to_date(paid_at), method order by row_seq desc) as row_seq_dense_rank,
        cume_dist() over (partition by order_id order by is_cash nulls first) as is_cash_cume_dist,
        avg(is_new_session) over (partition by order_id order by is_new_session, id rows between 2 preceding and 1 preceding) as is_new_session_avg_2_prior,
        count(next_amount) over (partition by order_id, status) as order_id_count_next_amount
    from metrics
)
select
    id,
    order_id,
    amount,
    paid_at,
    status,
    is_new_session,
    is_cash_cume_dist
from scored
