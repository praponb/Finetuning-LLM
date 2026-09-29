SELECT i.id, i.quantity, i.store_id, p.cost FROM inventory i INNER JOIN products p ON p.id = i.product_id WHERE p.brand IN ('Globex', 'Umbrella', 'Acme');
