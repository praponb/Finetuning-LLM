with scored as (
    select
        *,
        lag(amount) over w_recent as prev_amount,
        lead(amount) over w_recent as next_amount,
        row_number() over w_recent as seq_no
    from payments
    where amount > 600
    window w_recent as (partition by order_id order by paid_at, id)
)
select
    id,
    order_id,
    amount,
    status,
    next_amount,
    seq_no
from scored
order by status nulls first;
