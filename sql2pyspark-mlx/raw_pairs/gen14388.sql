SELECT
    CASE WHEN sqft >= 16642 THEN 'high' WHEN sqft >= 7538 THEN 'mid' ELSE 'low' END AS sqft_bucket,
    SUM(CASE WHEN region = 'South' THEN 1 ELSE 0 END) AS south_count,
    MIN(opened_date) AS min_opened_date,
    SUM(sqft) AS total_sqft
FROM stores
GROUP BY 1
ORDER BY sqft_bucket DESC
