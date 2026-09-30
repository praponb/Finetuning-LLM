select order_id, quantity
from order_items
where quantity between 5 and 6
except
select order_id, quantity
from order_items
where product_id is not null
order by order_id;
