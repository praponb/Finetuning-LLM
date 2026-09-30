SELECT *
FROM (
    SELECT CASE WHEN price >= 910 THEN 'high' WHEN price >= 510 THEN 'mid' ELSE 'low' END AS price_bucket, SUM(CASE WHEN brand = 'Initech' THEN 1 ELSE 0 END) AS initech_count, SUM(CASE WHEN category = 'Electronics' THEN cost ELSE 0 END) AS electronics_cost
    FROM products
    GROUP BY CASE WHEN price >= 910 THEN 'high' WHEN price >= 510 THEN 'mid' ELSE 'low' END
) AS x
WHERE initech_count > 80
ORDER BY electronics_cost
