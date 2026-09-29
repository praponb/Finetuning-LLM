SELECT category, max_stock_qty, avg_price
FROM (
    SELECT category, MAX(stock_qty) AS max_stock_qty, ROUND(AVG(price), 2) AS avg_price
    FROM products
    GROUP BY category
) AS s
WHERE max_stock_qty > 130;
