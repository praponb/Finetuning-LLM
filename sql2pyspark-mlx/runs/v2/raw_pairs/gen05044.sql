SELECT brand, cost_bucket, price_count
FROM (
    SELECT brand, CASE WHEN cost >= 700 THEN 'high' WHEN cost >= 190 THEN 'mid' ELSE 'low' END AS cost_bucket, COUNT(price) AS price_count
    FROM products
    GROUP BY brand, CASE WHEN cost >= 700 THEN 'high' WHEN cost >= 190 THEN 'mid' ELSE 'low' END
) AS s
WHERE price_count > 4
ORDER BY brand ASC;
