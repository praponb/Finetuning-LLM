SELECT brand, acme_count
FROM (
    SELECT brand, SUM(CASE WHEN brand = 'Acme' THEN 1 ELSE 0 END) AS acme_count
    FROM products
    GROUP BY brand
) AS x
WHERE acme_count >= 75
ORDER BY brand, acme_count
LIMIT 10;
