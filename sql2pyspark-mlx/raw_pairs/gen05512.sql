SELECT CASE WHEN brand IN ('Initech', 'Umbrella') THEN 'group_a' ELSE 'other' END AS brand_group, COUNT(DISTINCT price) AS unique_brands, AVG(cost) AS avg_cost
FROM products
WHERE price BETWEEN 640 AND 1000 OR price > 500
GROUP BY CASE WHEN brand IN ('Initech', 'Umbrella') THEN 'group_a' ELSE 'other' END
