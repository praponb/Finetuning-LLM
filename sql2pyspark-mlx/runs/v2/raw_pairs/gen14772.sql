select o.id, o.channel from orders o where o.store_id in (23, 25, 27, 32) and exists (select 1 from order_items oi where oi.order_id = o.id and oi.quantity is null) order by channel, o.id desc;
