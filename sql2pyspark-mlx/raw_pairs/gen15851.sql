with scored as (
    select
        *,
        ntile(10) over (partition by method, status order by amount desc, id) as amount_bucket,
        avg(amount) over w_ordered as method_avg_amount
    from payments
    window w_ordered as (partition by method, status)
)
select
    id,
    method,
    paid_at,
    status,
    amount_bucket
from scored
order by amount_bucket
