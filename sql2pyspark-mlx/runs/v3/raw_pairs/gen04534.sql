select orders.id, orders.store_id, orders.discount
from orders
where exists (select 1 from order_items where order_items.order_id = orders.id and order_items.quantity = 6);
