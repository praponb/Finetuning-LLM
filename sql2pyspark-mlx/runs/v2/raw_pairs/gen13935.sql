select
    status,
    max(opened_date) as last_opened_date,
    max(balance) - min(balance) as balance_range,
    min(opened_date) as first_opened_date
from accounts
group by status
having max(balance) - min(balance) > 51
order by balance_range asc, status asc
limit 3
