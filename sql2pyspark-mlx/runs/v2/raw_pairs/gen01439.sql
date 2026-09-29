select customer_id, status
from accounts
where status in ('CLOSED', 'FROZEN')
union all
select customer_id, status
from accounts
where month(opened_date) in (2, 7, 12)
order by status desc
