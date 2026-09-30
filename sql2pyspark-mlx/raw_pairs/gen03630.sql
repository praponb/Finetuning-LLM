SELECT *
FROM (
    SELECT brand, MIN(launch_date) AS min_launch_date, SUM(CASE WHEN category = 'Fashion' THEN 1 ELSE 0 END) AS fashion_count
    FROM products
    GROUP BY brand
) AS agg
WHERE fashion_count >= 67
ORDER BY brand;
