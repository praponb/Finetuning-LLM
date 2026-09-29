SELECT brand, avg_stock_qty, cnt
FROM (
    SELECT brand, AVG(stock_qty) AS avg_stock_qty, COUNT(*) AS cnt
    FROM products
    GROUP BY brand
) AS x
WHERE cnt < 5;
