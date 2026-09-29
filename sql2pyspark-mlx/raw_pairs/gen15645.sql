select id, opened_date, balance
from accounts
where opened_date > (select max(opened_date) from accounts)
order by balance
