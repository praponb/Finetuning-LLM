SELECT product_id FROM inventory WHERE quantity <> 87 UNION SELECT product_id FROM inventory WHERE last_restock > '2025-03-04';
