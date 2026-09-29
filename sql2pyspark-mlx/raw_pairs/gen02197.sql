select status
from orders
union all
select status
from subscriptions
where status = 'CANCELLED'
order by status
