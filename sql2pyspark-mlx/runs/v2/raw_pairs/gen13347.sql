SELECT p.id, p.cost, p.brand FROM products p WHERE p.id IN (SELECT product_id FROM store_sales WHERE product_id IN (4, 8, 9, 10))
