SELECT
    CASE WHEN cost >= 740 THEN 'high' WHEN cost >= 110 THEN 'mid' ELSE 'low' END AS cost_bucket,
    category,
    MAX(launch_date) AS last_launch_date,
    COUNT(CASE WHEN brand = 'Umbrella' THEN 1 END) AS umbrella_count,
    COUNT(*) AS cnt
FROM products
GROUP BY CASE WHEN cost >= 740 THEN 'high' WHEN cost >= 110 THEN 'mid' ELSE 'low' END, category
HAVING COUNT(CASE WHEN brand = 'Umbrella' THEN 1 END) > 2
ORDER BY category DESC, cost_bucket;
