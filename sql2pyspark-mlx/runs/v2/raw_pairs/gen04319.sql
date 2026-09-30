select
    year(opened_date) as opened_year,
    count(*) as account_count,
    sum(balance) as total_balance,
    count(balance) as balance_count
from accounts
where balance < 37000 and opened_date > '2024-11-13'
group by year(opened_date)
