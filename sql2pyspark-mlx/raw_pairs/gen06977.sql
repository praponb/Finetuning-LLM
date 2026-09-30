with flagged as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when lag(paid_at) over (partition by order_id order by paid_at asc, id asc) is null
                 or paid_at > lag(paid_at) over (partition by order_id order by paid_at asc, id asc) + interval '90' day then 1 else 0 end as session_start_flag
    from payments
),
sessions_payments as (
    -- Running sum of the flag = session number
    select *, sum(session_start_flag) over (partition by order_id order by paid_at asc, id asc rows between unbounded preceding and current row) as session_no
    from flagged
)
select
    s.id,
    s.order_id,
    s.method,
    s.amount,
    s.paid_at,
    s.status,
    s.session_start_flag,
    (select max(p2.amount) from payments p2 where p2.order_id = s.order_id) as max_payment_amount
from sessions_payments s;
