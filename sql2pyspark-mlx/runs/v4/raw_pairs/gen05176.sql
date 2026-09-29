select a.id, a.account_type, a.opened_date
from accounts a
where not exists (select 1 from transactions t where t.account_id = a.id)
