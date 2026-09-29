select status, date_trunc('MONTH', opened_date) as opened_month, round(stddev(balance), 2) as stddev_balance
from accounts
group by 1, 2
order by opened_month desc, status
limit 20;
