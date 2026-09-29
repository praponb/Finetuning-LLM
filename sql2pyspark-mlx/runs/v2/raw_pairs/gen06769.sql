with filtered_payments as (
    select *
    from payments
    where paid_at >= '2024-04-01'
),
agg as (
    select date_trunc('MONTH', paid_at) as paid_month, avg(amount) as avg_amount, max(amount) as max_amount
    from filtered_payments
    group by date_trunc('MONTH', paid_at)
)
select *
from agg
where max_amount >= 780
order by avg_amount asc, paid_month asc
limit 1
