select a.id, a.opened_date
from accounts a
where not exists (select 1 from transactions t where t.account_id = a.id and t.amount >= 3300)
order by a.id desc, a.opened_date
