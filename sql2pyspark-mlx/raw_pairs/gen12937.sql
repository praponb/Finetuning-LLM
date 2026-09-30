SELECT
    s.region,
    CASE WHEN s.sqft >= 16598 THEN 'high' WHEN s.sqft >= 4976 THEN 'mid' ELSE 'low' END AS sqft_bucket,
    SUM(CASE WHEN o.channel = 'mobile' THEN 1 ELSE 0 END) AS mobile_count,
    COUNT(*) AS row_count
FROM orders o
RIGHT JOIN stores s ON o.store_id = s.id
WHERE o.status <> 'CANCELLED'
GROUP BY s.region, CASE WHEN s.sqft >= 16598 THEN 'high' WHEN s.sqft >= 4976 THEN 'mid' ELSE 'low' END
ORDER BY row_count DESC, sqft_bucket, region
LIMIT 10
