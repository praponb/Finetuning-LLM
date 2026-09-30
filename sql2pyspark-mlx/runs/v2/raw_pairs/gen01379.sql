select a.id, a.order_id, a.quantity from order_items a where a.quantity = (select max(b.quantity) from order_items b where b.order_id = a.order_id);
