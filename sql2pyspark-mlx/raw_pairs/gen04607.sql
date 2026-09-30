select a.id, a.balance
from accounts a
where exists (select 1 from transactions t where t.account_id = a.id and t.txn_time >= '2023-09-01')
order by id;
