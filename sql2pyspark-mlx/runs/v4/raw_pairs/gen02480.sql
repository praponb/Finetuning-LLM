select order_date
from orders
except
select last_restock as order_date
from inventory;
