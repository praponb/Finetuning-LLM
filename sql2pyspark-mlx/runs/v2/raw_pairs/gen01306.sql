select a.id, a.status
from accounts a
where a.status is not null and not exists (select 1 from transactions t where t.account_id = a.id and t.amount between 2400 and 3400)
order by id desc
