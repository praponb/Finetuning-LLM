select order_id, status
from payments
where order_id is null
intersect
select order_id, status
from payments
where amount >= 1200
order by status;
