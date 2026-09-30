select id, order_id, quantity
from order_items
where order_id in (25, 32, 39)
union
select id, order_id, quantity
from order_items
where product_id in (25, 38, 40);
