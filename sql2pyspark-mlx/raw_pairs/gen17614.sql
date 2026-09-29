SELECT d.country, a.category, COUNT(*) AS num_rows, MIN(a.price) AS min_price FROM products a JOIN suppliers d ON a.supplier_id = d.id GROUP BY d.country, a.category ORDER BY category NULLS FIRST;
