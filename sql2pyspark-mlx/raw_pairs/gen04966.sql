select t.id, t.amount, t.txn_type
from transactions t
where t.account_id in (select id from accounts where balance >= 55000)
