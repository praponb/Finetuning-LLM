SELECT p.brand, p.launch_date, i.last_restock FROM products p LEFT JOIN inventory i ON p.id = i.product_id WHERE i.quantity BETWEEN 144 AND 146 AND i.store_id IN (2, 4, 5, 6);
