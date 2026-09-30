WITH recent_products AS (
    SELECT *
    FROM products
    WHERE launch_date BETWEEN '2023-06-01' AND '2024-07-17'
),
summary AS (
    SELECT category, COUNT(price) AS price_count, SUM(stock_qty) AS total_stock_qty
    FROM recent_products
    GROUP BY category
)
SELECT category, price_count, total_stock_qty
FROM summary
WHERE price_count < 7
ORDER BY total_stock_qty DESC, category
LIMIT 50;
