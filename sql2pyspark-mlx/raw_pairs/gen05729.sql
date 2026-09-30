select method, status, count(case when status = 'SUCCESS' then 1 end) as success_count
from payments
group by method, status
having count(case when status = 'SUCCESS' then 1 end) > 3
