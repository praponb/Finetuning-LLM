SELECT p.category, p.cost, p.price, i.quantity, i.store_id, i.product_id FROM products p LEFT OUTER JOIN inventory i ON p.id = i.product_id WHERE i.product_id IN (5, 6, 8)
