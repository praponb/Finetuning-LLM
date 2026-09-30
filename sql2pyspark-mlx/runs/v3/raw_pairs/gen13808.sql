SELECT id, category, price, CASE WHEN category = 'Books' THEN 1 ELSE 0 END AS is_books, FLOOR(cost) AS cost_whole FROM products
