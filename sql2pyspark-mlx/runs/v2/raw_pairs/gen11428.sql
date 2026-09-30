select a.id, a.status, a.opened_date
from accounts a
where exists (select 1 from transactions t where t.account_id = a.id and t.amount <= 1800)
