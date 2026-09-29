select *
from (
    select status, quarter(paid_at) as paid_quarter, round(stddev(amount), 2) as stddev_amount
    from payments
    where order_id is not null
    group by status, quarter(paid_at)
) as s
where stddev_amount < 55
order by stddev_amount desc, status, paid_quarter
limit 10
