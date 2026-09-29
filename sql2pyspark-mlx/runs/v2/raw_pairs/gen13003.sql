SELECT id, product_id FROM inventory WHERE last_restock BETWEEN '2024-02-01' AND '2025-03-29' INTERSECT SELECT id, product_id FROM inventory WHERE YEAR(last_restock) = 2024 ORDER BY id;
