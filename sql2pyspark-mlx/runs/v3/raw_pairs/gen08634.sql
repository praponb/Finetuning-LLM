select a.id, a.balance, a.status
from accounts a
where not exists (select 1 from transactions t where t.account_id = a.id and t.amount is not null)
