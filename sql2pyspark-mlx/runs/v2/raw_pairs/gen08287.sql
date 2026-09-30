with account_summary as (
    select account_id, min(amount) as min_amount, round(stddev(amount), 2) as stddev_amount
    from transactions
    group by account_id
)
select
    accounts.status,
    accounts.customer_id,
    account_summary.min_amount,
    account_summary.stddev_amount
from accounts
inner join account_summary on accounts.id = account_summary.account_id
order by account_summary.stddev_amount;
