select order_items.id, order_items.product_id, order_items.order_id
from order_items
where order_items.unit_price >= 170 and order_items.order_id not in (select id from orders where customer_id is not null)
