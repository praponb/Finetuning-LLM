SELECT
    city,
    CASE WHEN sqft >= 13710 THEN 'high' WHEN sqft >= 8273 THEN 'mid' ELSE 'low' END AS sqft_bucket,
    MAX(sqft) AS max_sqft,
    ROUND((100 * SUM(CASE WHEN region = 'Central' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_central
FROM stores
GROUP BY city, CASE WHEN sqft >= 13710 THEN 'high' WHEN sqft >= 8273 THEN 'mid' ELSE 'low' END
HAVING ROUND((100 * SUM(CASE WHEN region = 'Central' THEN 1 ELSE 0 END)) / COUNT(*), 1) >= 42
ORDER BY 2 DESC
