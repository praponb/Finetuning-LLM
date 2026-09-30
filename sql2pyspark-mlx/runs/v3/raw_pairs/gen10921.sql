with account_stats as (
    select account_id, count(*) total_count
    from transactions
    where txn_type = 'DEBIT'
    group by account_id
)
select a.id, a.customer_id, as.total_count
from accounts a
join account_stats as on a.id = as.account_id
where as.total_count >= 5
order by a.customer_id desc, as.total_count desc
