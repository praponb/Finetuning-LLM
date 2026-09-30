select amount
from orders
union
select unit_price as amount
from order_items
order by amount;
