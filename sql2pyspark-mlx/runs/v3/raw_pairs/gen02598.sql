select status
from orders
where status <> 'SHIPPED'
intersect
select status
from payments
where hour(paid_at) between 9 and 12;
