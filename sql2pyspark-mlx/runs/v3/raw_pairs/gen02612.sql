select id, product_id
from order_items
where order_id in (1, 8, 23)
intersect
select id, product_id
from order_items
where order_id in (20, 38)
order by product_id
