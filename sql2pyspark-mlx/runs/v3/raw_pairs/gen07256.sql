SELECT *
FROM (
    SELECT supplier_id, AVG(price) AS avg_price, MAX(price) - MIN(price) AS price_range
    FROM products
    GROUP BY supplier_id
) AS s
WHERE avg_price < 920
ORDER BY supplier_id DESC
