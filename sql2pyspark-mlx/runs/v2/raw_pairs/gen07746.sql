SELECT SUM(CASE WHEN category = 'Books' THEN 1 ELSE 0 END) AS books_count, COUNT(stock_qty) AS stock_qty_count
FROM products
ORDER BY stock_qty_count DESC, books_count DESC
