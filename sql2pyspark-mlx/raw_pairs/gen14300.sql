select
    t.id,
    t.txn_type,
    a.account_type,
    a.status,
    c.age,
    trim(t.txn_type) as clean_txn_type
from transactions t
join accounts a on t.account_id = a.id
full outer join customers c on a.customer_id = c.id
where a.account_type in ('CREDIT', 'CHECKING') and to_date(t.txn_time) < '2025-05-01'
