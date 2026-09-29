select
    status,
    min(opened_date) as first_opened_date,
    sum(balance) as total_balance,
    sum(case when status = 'CLOSED' then 1 else 0 end) as closed_count
from accounts
where customer_id in (1, 18, 25) and customer_id in (4, 25, 27, 33) and customer_id = 20
group by status
order by status
limit 10
