select o.id, o.amount from orders o where not exists (select 1 from order_items oi where oi.order_id = o.id);
