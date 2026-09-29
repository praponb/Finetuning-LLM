SELECT product_id FROM inventory WHERE last_restock BETWEEN '2023-04-01' AND '2023-12-01' EXCEPT SELECT product_id FROM inventory WHERE quantity > 168 ORDER BY product_id;
