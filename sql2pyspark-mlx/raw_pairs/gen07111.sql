select customer_id, count(case when status = 'FROZEN' then 1 end) frozen_count
from accounts
where balance <= 62000 and opened_date between '2024-07-10' and '2024-12-01'
group by customer_id;
