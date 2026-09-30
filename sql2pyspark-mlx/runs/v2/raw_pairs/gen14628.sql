select t.id, t.txn_type, t.merchant
from transactions t
where t.account_id not in (select id from accounts where account_type in ('LOAN', 'CREDIT'))
order by t.merchant
