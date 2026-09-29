select method, status
from payments
where method is null
intersect
select method, status
from payments
where order_id is not null
order by status
