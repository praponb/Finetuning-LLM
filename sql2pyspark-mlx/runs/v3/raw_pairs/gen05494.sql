select
    t.id,
    t.txn_time,
    a.balance,
    a.account_type,
    c.name
from transactions t
left outer join accounts a on t.account_id = a.id
inner join customers c on a.customer_id = c.id
