select customer_id, count(case when status = 'CLOSED' then 1 end) as closed_count
from accounts
where status is null and balance <= 63000 and balance < 71000
group by customer_id
order by closed_count asc;
