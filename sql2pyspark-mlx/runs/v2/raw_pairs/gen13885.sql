with valid_payments as (
    select *, upper(method) method_upper
    from payments
    where to_date(paid_at) >= '2024-07-16'
),
agg as (
    select hour(paid_at) paid_hour, round(stddev(amount), 2) stddev_amount
    from valid_payments
    group by hour(paid_at)
)
select *
from agg
order by stddev_amount
