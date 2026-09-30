select status, year(opened_date) as opened_year, count(balance) as balance_count
from accounts
where status in ('OPEN', 'CLOSED', 'FROZEN')
group by 1, 2
