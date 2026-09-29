select order_date, 'order' as source
from orders
union all
select last_restock, 'inventory' as source
from inventory
