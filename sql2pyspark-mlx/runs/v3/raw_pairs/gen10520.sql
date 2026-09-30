with account_stats as (
    select account_id, max(amount) max_amount
    from transactions
    where merchant = 'Amazon'
    group by account_id
)
select a.id, a.customer_id, as.max_amount
from accounts a
left join account_stats as on a.id = as.account_id
order by max_amount desc
