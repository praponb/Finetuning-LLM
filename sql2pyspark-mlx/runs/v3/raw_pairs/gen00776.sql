SELECT
    region,
    DATE_FORMAT(opened_date, 'yyyy-MM') opened_month,
    CASE WHEN sqft >= 14294 THEN 'high' WHEN sqft >= 4994 THEN 'mid' ELSE 'low' END sqft_bucket,
    COUNT(DISTINCT sqft) unique_regions,
    COUNT(*) store_count
FROM stores
WHERE sqft <= 9685 OR region NOT IN ('West', 'North', 'Central')
GROUP BY region, DATE_FORMAT(opened_date, 'yyyy-MM'), CASE WHEN sqft >= 14294 THEN 'high' WHEN sqft >= 4994 THEN 'mid' ELSE 'low' END
HAVING COUNT(*) >= 4
ORDER BY unique_regions
