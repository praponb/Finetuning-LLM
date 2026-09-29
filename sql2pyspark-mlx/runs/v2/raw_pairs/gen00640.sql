with base as (
    select *, to_date(paid_at) paid_at_date
    from payments
    where status = 'REFUNDED' and paid_at < '2023-09-01' and amount < 1010
),
payments_stats as (
    select method, count(*) payment_count
    from base
    group by method
)
select method, payment_count
from payments_stats
where payment_count > 4
order by payment_count desc, method asc
limit 50;
