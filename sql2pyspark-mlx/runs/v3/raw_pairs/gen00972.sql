with transactions_by_account as (
    select account_id, max(amount) - min(amount) as amount_range, avg(amount) as avg_amount
    from transactions
    where account_id = 3
    group by account_id
)
select
    a.id,
    a.opened_date,
    a.customer_id,
    tba.amount_range,
    tba.avg_amount
from accounts a
left join transactions_by_account tba on a.id = tba.account_id
order by amount_range desc
