select date_format(opened_date, 'yyyy-MM') as opened_month, max(opened_date) as latest_opened_date, max(balance) - min(balance) as balance_range
from accounts
group by date_format(opened_date, 'yyyy-MM')
having max(balance) - min(balance) > 70
order by balance_range desc
