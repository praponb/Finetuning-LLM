with cleaned as (
    select *, quarter(paid_at) as paid_at_quarter
    from payments
    where paid_at > to_date('2023-09-05')
      and order_id is not null
),
with_gaps as (
    select *, case when paid_at - lag(paid_at) over (partition by order_id order by paid_at asc, id asc) > interval 90 days then 1 else 0 end as session_start_flag
    from cleaned
),
sessions_payments as (
    select
        id,
        order_id,
        method,
        amount,
        paid_at,
        sum(session_start_flag) over (partition by order_id order by paid_at asc, id asc rows between unbounded preceding and current row) as burst_id
    from with_gaps
)
select
    burst_id,
    count(case when method = 'promptpay' then 1 end) as promptpay_count,
    max(amount) as max_amount,
    percentile_approx(amount, 0.9) as amount_p90
from sessions_payments
group by burst_id
order by promptpay_count desc nulls first
