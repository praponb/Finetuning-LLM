with account_stats as (
    select account_id, avg(amount) as avg_amount, count(amount) as amount_count
    from transactions
    where to_date(txn_time) < '2023-08-01'
    group by account_id
)
select
    a.id,
    a.status,
    a.balance,
    as.avg_amount,
    as.amount_count
from accounts a
inner join account_stats as on a.id = as.account_id
where as.amount_count >= 6
order by a.status desc
