with with_gaps as (
    select *, case when lag(paid_at) over (partition by status order by paid_at, id) is null
                 or paid_at > lag(paid_at) over (partition by status order by paid_at, id) + interval 14 days then 1 else 0 end as is_new_session
    from payments
),
islands as (
    select *, sum(is_new_session) over (partition by status order by paid_at, id rows between unbounded preceding and current row) as session_id
    from with_gaps
)
select
    s.id,
    s.order_id,
    s.amount,
    s.paid_at,
    s.status,
    s.is_new_session,
    case when status rlike '^(FAILED|REFUNDED)$' then 'PRIORITY' when status rlike '^(SUCCESS)$' then 'CORE' else 'GROUP_B' end as status_zone,
    (select sum(t2.amount) from payments t2 where t2.order_id = s.order_id) as sum_payment_amount
from islands s
order by paid_at;
