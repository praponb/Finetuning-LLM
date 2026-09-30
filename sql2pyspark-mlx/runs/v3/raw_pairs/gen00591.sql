select accounts.id, accounts.opened_date
from accounts
where exists (select 1 from transactions where transactions.account_id = accounts.id and transactions.txn_type in ('REFUND', 'DEBIT'))
order by id asc
