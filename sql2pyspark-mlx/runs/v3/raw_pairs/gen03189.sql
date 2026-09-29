select *, add_months(opened_date, 1) as opened_date_plus_months
from accounts
order by balance desc, id
limit 50
