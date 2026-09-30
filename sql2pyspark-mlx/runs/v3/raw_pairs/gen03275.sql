SELECT SUM(CASE WHEN category = 'Books' THEN price ELSE 0 END) AS books_price
FROM products
ORDER BY books_price DESC
