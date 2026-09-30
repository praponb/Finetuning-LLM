SELECT *
FROM (
    SELECT category, COUNT(DISTINCT price) AS unique_brands, SUM(CASE WHEN brand = 'Initech' THEN 1 ELSE 0 END) AS initech_count
    FROM products
    GROUP BY category
) AS x
WHERE initech_count > 44
ORDER BY unique_brands DESC
