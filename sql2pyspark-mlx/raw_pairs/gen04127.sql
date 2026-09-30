select orders.id, orders.store_id
from orders
where not exists (select 1 from order_items where order_items.order_id = orders.id and order_items.product_id = 39);
