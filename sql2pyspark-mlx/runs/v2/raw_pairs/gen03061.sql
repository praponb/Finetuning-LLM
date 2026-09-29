select
    month(opened_date) as opened_month,
    status,
    count(case when status = 'OPEN' then 1 end) as open_count,
    round(avg(balance), 2) as avg_balance
from accounts
where balance < 56000
group by month(opened_date), status
having open_count >= 3
order by avg_balance desc, opened_month
