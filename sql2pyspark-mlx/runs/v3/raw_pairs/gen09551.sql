select id, quantity, order_id
from order_items
where order_id in (29, 34)
except
select id, quantity, order_id
from order_items
where quantity between 4 and 8
order by order_id
