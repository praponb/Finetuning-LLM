select status
from orders
where status in ('SHIPPED', 'CANCELLED', 'COMPLETED')
except
select status
from payments
where amount between 1250 and 1330;
