select
    t.txn_type,
    t.amount,
    a.customer_id,
    a.balance,
    a.status
from transactions t
join accounts a on t.account_id = a.id
order by amount desc
