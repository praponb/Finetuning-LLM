select oi.id, oi.order_id, oi.product_id, p.supplier_id, p.product_name, p.price from order_items oi join products p on oi.product_id = p.id where oi.quantity = 5 and p.supplier_id is not null
