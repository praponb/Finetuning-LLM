select status
from orders
union all
select status
from payments
where amount >= 1310;
