select
    status,
    paid_day,
    cash_count,
    avg_amount
from (
    select
        status,
        to_date(paid_at) as paid_day,
        sum(case when method = 'cash' then 1 else 0 end) as cash_count,
        round(avg(amount), 2) as avg_amount
    from payments
    where order_id in (23, 25)
    group by status, to_date(paid_at)
) as s
where avg_amount < 31
order by paid_day
