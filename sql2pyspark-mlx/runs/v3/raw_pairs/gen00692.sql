SELECT
    category,
    MONTH(launch_date) launch_month,
    brand,
    MIN(stock_qty) min_stock_qty,
    MAX(cost) max_cost
FROM products
WHERE price < 1160
GROUP BY 1, 2, 3
ORDER BY brand ASC, max_cost DESC
