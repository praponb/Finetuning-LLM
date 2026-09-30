select opened_date
from stores
intersect
select order_date
from orders
order by opened_date desc
