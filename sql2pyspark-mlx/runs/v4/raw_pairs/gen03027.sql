SELECT
    s.city,
    CASE WHEN s.sqft >= 14872 THEN 'high' WHEN s.sqft >= 2863 THEN 'mid' ELSE 'low' END AS sqft_bucket,
    ROUND(AVG(ss.units), 2) AS avg_units,
    COUNT(*) AS row_count
FROM store_sales ss
LEFT JOIN stores s ON ss.store_id = s.id
FULL OUTER JOIN products p ON ss.product_id = p.id
WHERE p.category = 'Grocery'
GROUP BY s.city, CASE WHEN s.sqft >= 14872 THEN 'high' WHEN s.sqft >= 2863 THEN 'mid' ELSE 'low' END
ORDER BY row_count DESC, city, sqft_bucket
LIMIT 3
