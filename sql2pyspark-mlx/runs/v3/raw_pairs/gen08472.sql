select order_date
from orders
union all
select last_restock
from inventory
order by order_date;
