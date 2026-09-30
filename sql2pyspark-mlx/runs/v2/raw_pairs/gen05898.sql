with transactions_by_account as (
    select account_id, round(stddev(amount), 2) as stddev_amount, avg(amount) as avg_amount
    from transactions
    where to_date(txn_time) < '2023-05-01'
    group by account_id
)
select
    a.id,
    a.opened_date,
    a.customer_id,
    tba.stddev_amount,
    tba.avg_amount
from accounts a
join transactions_by_account tba on a.id = tba.account_id
order by tba.avg_amount;
