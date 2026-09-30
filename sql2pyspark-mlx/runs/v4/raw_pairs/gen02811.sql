SELECT
    category,
    SUM(stock_qty) AS total_stock_qty,
    COUNT(*) AS cnt,
    SUM(SUM(stock_qty)) OVER (ORDER BY category) AS cumulative_stock_qty
FROM products
GROUP BY category
ORDER BY cnt DESC, category
LIMIT 10
