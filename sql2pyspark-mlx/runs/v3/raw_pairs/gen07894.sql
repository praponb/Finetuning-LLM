select a.id, a.opened_date
from accounts a
where not exists (select 1 from transactions t where t.account_id = a.id and to_date(t.txn_time) < '2025-10-01')
order by a.opened_date asc, id desc
limit 5;
