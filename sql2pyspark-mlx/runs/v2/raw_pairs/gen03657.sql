SELECT
    CASE WHEN cost >= 590 THEN 'high' WHEN cost >= 110 THEN 'mid' ELSE 'low' END cost_bucket,
    category,
    COUNT(*) row_count,
    COUNT(CASE WHEN brand = 'Globex' THEN 1 END) globex_count
FROM products
GROUP BY 1, 2
ORDER BY globex_count DESC, cost_bucket, category
LIMIT 50;
