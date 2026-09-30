with prev_events as (
    select *, lag(paid_at) over (partition by order_id order by paid_at, id) as prev_paid_at
    from payments
),
gaps as (
    select *, case when prev_paid_at is null or paid_at > prev_paid_at + interval 60 days then 1 else 0 end as is_new_session
    from prev_events
),
islands as (
    select *, sum(is_new_session) over (partition by order_id order by paid_at, id rows between unbounded preceding and current row) as session_id
    from gaps
)
select
    status,
    case when is_new_session >= 1 then 'LARGE' else 'SMALL' end as is_new_session_tier,
    count(*) filter (where status = 'REFUNDED') as refunded_rows,
    count_if(status = 'REFUNDED') as refunded_events,
    sum(case when method = 'promptpay' then session_id else 0 end) as promptpay_session_id
from islands
group by status, case when is_new_session >= 1 then 'LARGE' else 'SMALL' end
