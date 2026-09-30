select accounts.id, accounts.account_type, accounts.status
from accounts
where not exists (select 1 from transactions where transactions.account_id = accounts.id and transactions.account_id = 23)
