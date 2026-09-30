select accounts.id, accounts.opened_date, accounts.account_type
from accounts
where exists (select 1 from transactions where transactions.account_id = accounts.id)
order by opened_date desc, id asc
limit 10
