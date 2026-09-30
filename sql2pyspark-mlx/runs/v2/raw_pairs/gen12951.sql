select status
from orders
where customer_id is null
union all
select status
from accounts
where balance < 47000
order by status
