SELECT brand, category, COUNT(CASE WHEN category = 'Books' THEN 1 END) AS books_count, MAX(cost) AS max_cost FROM products GROUP BY brand, category HAVING MAX(cost) >= 510;
