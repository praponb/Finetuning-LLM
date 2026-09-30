select month(opened_date) as opened_month, max(balance) - min(balance) as balance_range
from accounts
group by month(opened_date)
order by balance_range desc;
