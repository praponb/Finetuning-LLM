SELECT SUM(cost) AS total_cost, COUNT(CASE WHEN category = 'Books' THEN 1 END) AS books_count FROM products ORDER BY 1 DESC, books_count DESC
