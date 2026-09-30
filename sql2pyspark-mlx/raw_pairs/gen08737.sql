with prev_events as (
    -- Previous event time per key
    select *, lag(paid_at) over (partition by order_id order by paid_at, id) as prev_paid_at
    from payments
),
flagged as (
    -- Flag rows that start a new session
    select *, case when prev_paid_at is null or paid_at > prev_paid_at + interval 7 days then 1 else 0 end as is_new_burst
    from prev_events
),
islands as (
    -- Running sum of the flag = session number
    select *, sum(is_new_burst) over (partition by order_id order by paid_at, id rows between unbounded preceding and current row) as session_id
    from flagged
),
enriched as (
    -- Attach orders attributes
    select
        src.*,
        o.status as order_status,
        o.order_date
    from islands src
    left join orders o
        on src.order_id = o.id
),
with_prev as (
    -- Previous event time per key
    select *, lag(paid_at) over (partition by status order by paid_at, id) as prev_paid_at_2
    from enriched
),
gaps as (
    -- Flag rows that start a new session
    select *, case when prev_paid_at_2 is null or paid_at > prev_paid_at_2 + interval 14 days then 1 else 0 end as is_new_burst_2
    from with_prev
),
islands_payments as (
    -- Running sum of the flag = session number
    select *, sum(is_new_burst_2) over (partition by status order by paid_at, id rows between unbounded preceding and current row) as session_no
    from gaps
),
scored as (
    -- Window metrics over several different partitions
    select
        *,
        min(is_new_burst_2) over (partition by order_id order by prev_paid_at, id rows between 3 preceding and current row) as last4_min_is_new_burst_2,
        count(is_new_burst_2) over (partition by order_id order by prev_paid_at, id rows between 3 preceding and current row) as last4_count_is_new_burst_2,
        max(is_new_burst_2) over (partition by order_id order by prev_paid_at, id rows between 3 preceding and current row) as last4_max_is_new_burst_2,
        sum(is_new_burst_2) over (partition by order_id order by prev_paid_at, id rows between 3 preceding and current row) as last4_sum_is_new_burst_2,
        sum(session_no) over (partition by order_status order by order_date, id rows between unbounded preceding and current row) as running_session_no
    from islands_payments
),
with_windows as (
    -- Window metrics over several different partitions
    select
        *,
        avg(last4_sum_is_new_burst_2) over w_ordered as running_avg_last4_sum_is_new_burst_2,
        sum(last4_sum_is_new_burst_2) over w_ordered as running_sum_last4_sum_is_new_burst_2,
        count(last4_sum_is_new_burst_2) over w_ordered as running_count_last4_sum_is_new_burst_2
    from scored
    where method is not null
    window w_ordered as (partition by order_id order by running_session_no, id rows between unbounded preceding and current row)
),
cleaned as (
    -- Keep recent, valid rows
    select
        *,
        coalesce(last4_count_is_new_burst_2, 0) as last4_count_is_new_burst_2_filled,
        coalesce(method, 'UNKNOWN') as method_clean
    from with_windows
    where session_id between 5 and 8
),
cleaned_payments as (
    -- Keep recent, valid rows
    select
        *,
        least(is_new_burst_2, session_no) as least_is_new_burst_2_session_no,
        date_trunc('MONTH', order_date) as order_date_period,
        coalesce(order_status, 'UNKNOWN') as resolved_order_status
    from cleaned
    where method is not distinct from 'promptpay'
)
-- Window metrics over several different partitions
select
    id,
    order_id,
    amount,
    paid_at,
    status,
    prev_paid_at,
    is_new_burst,
    order_date,
    is_new_burst_2,
    last4_min_is_new_burst_2,
    last4_count_is_new_burst_2,
    last4_max_is_new_burst_2,
    running_session_no,
    running_avg_last4_sum_is_new_burst_2,
    last4_count_is_new_burst_2_filled,
    method_clean,
    order_date_period,
    dense_rank() over (partition by year(prev_paid_at), method order by last4_min_is_new_burst_2 nulls first) as last4_min_is_new_burst_2_dense_rank,
    first_value(least_is_new_burst_2_session_no, true) over (partition by to_date(prev_paid_at), order_status order by last4_count_is_new_burst_2_filled, id rows between unbounded preceding and current row) as first_known_least_is_new_burst_2_session_no,
    lag(session_id, 2) over (partition by method order by is_new_burst_2, id) as prev_session_id
from cleaned_payments
