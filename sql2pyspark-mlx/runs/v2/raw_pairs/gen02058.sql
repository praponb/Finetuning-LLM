select
    month(opened_date) as opened_month,
    status,
    count(balance) as balance_count,
    min(balance) as min_balance
from accounts
group by month(opened_date), status
having count(balance) >= 1
order by balance_count;
