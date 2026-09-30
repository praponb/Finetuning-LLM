select
    t.id,
    t.account_id,
    t.txn_type,
    a.customer_id
from transactions t
inner join accounts a on t.account_id = a.id
where t.account_id in (10, 14, 22, 39);
