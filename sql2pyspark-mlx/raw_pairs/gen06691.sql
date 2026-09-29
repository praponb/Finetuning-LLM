with filtered_payments as (
    select *
    from payments
    where amount < 1500
),
agg as (
    select
        date_trunc('MONTH', paid_at) as paid_month,
        method,
        round(avg(amount), 2) as avg_amount,
        sum(amount) as total_amount
    from filtered_payments
    group by date_trunc('MONTH', paid_at), method
)
select *
from agg;
