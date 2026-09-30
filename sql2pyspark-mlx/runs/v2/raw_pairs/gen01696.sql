select status, 'order' as source
from orders
where status in ('PENDING', 'SHIPPED', 'COMPLETED')
union
select status, 'account' as source
from accounts
order by status desc
