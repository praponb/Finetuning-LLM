SELECT *
FROM (
    SELECT brand, MIN(stock_qty) AS min_stock_qty, SUM(price) AS total_price
    FROM products
    WHERE supplier_id IS NOT NULL
    GROUP BY brand
) AS t
WHERE min_stock_qty > 280
ORDER BY total_price;
