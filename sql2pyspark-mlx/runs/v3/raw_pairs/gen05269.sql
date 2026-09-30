SELECT p.id, p.cost, p.price, p.supplier_id FROM products p WHERE p.id IN (SELECT product_id FROM order_items)
