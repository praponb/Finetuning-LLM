select p.id, p.product_name, p.supplier_id from products p where p.cost between 390 and 610 and not exists (select 1 from order_items oi where oi.product_id = p.id)
