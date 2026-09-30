select
    t.id,
    t.txn_type,
    a.balance,
    a.account_type
from transactions t
full outer join accounts a on a.id = t.account_id
where a.balance between 34000 and 44000
