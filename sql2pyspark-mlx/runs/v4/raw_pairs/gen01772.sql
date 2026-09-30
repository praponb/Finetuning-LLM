with with_windows as (
    -- Running and moving aggregates
    select
        *,
        sum(amount) over w_ordered as cumulative_amount,
        ntile(5) over (partition by status order by amount, id) as amount_bucket
    from payments
    window w_ordered as (partition by order_id order by paid_at, id rows between unbounded preceding and current row)
)
select
    id,
    order_id,
    amount,
    paid_at,
    cumulative_amount,
    amount_bucket
from with_windows
order by amount
