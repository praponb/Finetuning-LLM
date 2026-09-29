select month(opened_date) as opened_month, status, max(opened_date) as last_opened_date
from accounts
group by month(opened_date), status
order by last_opened_date
