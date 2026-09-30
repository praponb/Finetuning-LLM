select o.id, o.store_id, o.shipping_fee, o.status from orders o where not exists (select 1 from order_items oi where oi.order_id = o.id and oi.order_id = 21);
