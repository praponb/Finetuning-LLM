select status, channel
from orders
where status in ('COMPLETED', 'SHIPPED', 'PENDING')
union
select status, channel
from orders
where store_id = 28
order by status
