select a.id, a.account_type
from accounts a
where not exists (select 1 from transactions t where t.account_id = a.id and t.txn_type in ('CREDIT', 'FEE', 'DEBIT'))
order by account_type;
