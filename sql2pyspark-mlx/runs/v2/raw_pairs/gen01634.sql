with account_summary as (
    select account_id, round(avg(amount), 2) as avg_amount, max(amount) as max_amount
    from transactions
    where txn_type = 'CREDIT'
    group by account_id
)
select
    a.id,
    a.customer_id,
    as.avg_amount,
    as.max_amount
from accounts a
join account_summary as on a.id = as.account_id
order by id desc;
