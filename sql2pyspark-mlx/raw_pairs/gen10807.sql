with with_gaps as (
    select *, case when paid_at - lag(paid_at) over (partition by status order by paid_at, id) > interval 60 days then 1 else 0 end as session_start_flag
    from payments
),
sessions_payments as (
    select
        id,
        order_id,
        amount,
        paid_at,
        status,
        sum(session_start_flag) over (partition by status order by paid_at, id rows between unbounded preceding and current row) as session_id
    from with_gaps
)
select
    s.id,
    s.order_id,
    s.amount,
    s.session_id,
    (select max(p2.amount) from payments p2 where p2.order_id = s.order_id) as max_payment_amount
from sessions_payments s;
