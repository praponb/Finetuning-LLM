select p.id, p.product_name from products p where exists (select 1 from order_items oi where oi.product_id = p.id and oi.product_id in (20, 28))
