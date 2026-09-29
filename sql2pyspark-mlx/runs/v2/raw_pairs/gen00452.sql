select order_date, channel
from orders
where amount > 1420
union
select order_date, channel
from orders
where status not in ('PENDING', 'COMPLETED', 'RETURNED')
order by channel desc
