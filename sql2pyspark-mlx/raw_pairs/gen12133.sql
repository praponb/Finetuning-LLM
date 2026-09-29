select p.id, p.brand, p.category from products p where not exists (select 1 from order_items oi where oi.product_id = p.id)
