select t.id, t.txn_type
from transactions t
where t.txn_time < '2023-07-01' and t.account_id not in (select id from accounts where status = 'CLOSED')
order by txn_type
