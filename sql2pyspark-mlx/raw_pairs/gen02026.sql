with gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when lag(paid_at) over (partition by status order by paid_at, id) is null
                 or paid_at > lag(paid_at) over (partition by status order by paid_at, id) + interval 60 days then 1 else 0 end as is_new_session
    from payments
),
sessions_payments as (
    -- Running sum of the flag = session number
    select *, sum(is_new_session) over (partition by status order by paid_at, id rows between unbounded preceding and current row) as session_id
    from gaps
),
enriched as (
    -- Running and moving aggregates
    select
        *,
        lag(session_id, 1) over (order by is_new_session, id) as prev_session_id,
        sum(is_new_session) over (partition by month(paid_at), method order by session_id, id rows between unbounded preceding and current row) as running_is_new_session,
        avg(is_new_session) over (partition by status, order_id order by session_id, id rows between 6 preceding and 1 preceding) as is_new_session_avg_6_prior,
        last_value(method, true) over (partition by order_id order by session_id, id rows between unbounded preceding and current row) as last_known_method
    from sessions_payments
),
enriched_payments as (
    -- Attach orders attributes
    select e.*, p.order_date
    from enriched e
    left join orders p
        on e.order_id = p.id
),
enriched_payments_2 as (
    -- Ranking and gap features
    select
        id,
        method,
        status,
        session_id,
        prev_session_id,
        running_is_new_session,
        is_new_session_avg_6_prior,
        last_known_method,
        min(is_new_session) over (partition by order_id order by order_date, id rows between unbounded preceding and current row) as running_min_is_new_session,
        avg(is_new_session) over (partition by order_id order by order_date, id rows between unbounded preceding and current row) as running_avg_is_new_session,
        sum(is_new_session) over (partition by order_id order by order_date, id rows between unbounded preceding and current row) as running_sum_is_new_session,
        max(is_new_session) over (partition by order_id order by order_date, id rows between unbounded preceding and current row) as running_max_is_new_session,
        lead(prev_session_id, 2) over (order by prev_session_id, id) as next_prev_session_id
    from enriched_payments
    where order_id in (10, 11, 39, 40)
),
with_windows as (
    -- Running and moving aggregates
    select
        id,
        method,
        status,
        session_id,
        running_min_is_new_session,
        next_prev_session_id,
        max(running_avg_is_new_session) over (partition by method order by running_avg_is_new_session, id rows between 3 preceding and current row) as last4_max_running_avg_is_new_session,
        count(running_avg_is_new_session) over (partition by method order by running_avg_is_new_session, id rows between 3 preceding and current row) as last4_count_running_avg_is_new_session,
        sum(running_avg_is_new_session) over (partition by method order by running_avg_is_new_session, id rows between 3 preceding and current row) as last4_sum_running_avg_is_new_session,
        min(running_avg_is_new_session) over (partition by method order by running_avg_is_new_session, id rows between 3 preceding and current row) as last4_min_running_avg_is_new_session
    from enriched_payments_2
)
-- Ranking and gap features
select
    id,
    status,
    session_id,
    running_min_is_new_session,
    last4_min_running_avg_is_new_session,
    lead(last4_max_running_avg_is_new_session) over (partition by status, method order by last4_min_running_avg_is_new_session, id) as next_last4_max_running_avg_is_new_session,
    lag(next_prev_session_id) over (partition by status, method order by last4_min_running_avg_is_new_session, id) as prev_next_prev_session_id,
    lead(next_prev_session_id) over (partition by status, method order by last4_min_running_avg_is_new_session, id) as next_next_prev_session_id,
    row_number() over (partition by status, method order by last4_min_running_avg_is_new_session, id) as event_seq,
    percent_rank() over (order by last4_min_running_avg_is_new_session desc nulls last) as last4_min_running_avg_is_new_session_percent_rank
from with_windows
