SELECT *
FROM (
    SELECT brand, COUNT(DISTINCT cost) AS unique_suppliers, MAX(stock_qty) AS max_stock_qty
    FROM products
    GROUP BY brand
) AS t
WHERE max_stock_qty >= 260
ORDER BY max_stock_qty DESC;
