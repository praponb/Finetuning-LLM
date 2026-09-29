select status, 'order' source
from orders
where month(order_date) in (3, 9, 10)
union
select status, 'account' source
from accounts
where balance > 42000
order by status desc;
