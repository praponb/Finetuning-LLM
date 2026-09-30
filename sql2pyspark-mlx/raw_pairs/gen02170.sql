select id, product_id, order_id
from order_items
where order_id in (28, 37)
intersect
select id, product_id, order_id
from order_items
where product_id in (3, 33, 36)
