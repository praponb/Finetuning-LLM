SELECT
    region,
    QUARTER(opened_date) AS opened_quarter,
    SUM(CASE WHEN city = 'Singapore' THEN 1 ELSE 0 END) AS singapore_count,
    MAX(sqft) - MIN(sqft) AS sqft_range,
    ROUND((100 * SUM(CASE WHEN city = 'Chiang Mai' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_chiang_mai
FROM stores
GROUP BY region, QUARTER(opened_date)
HAVING SUM(CASE WHEN city = 'Singapore' THEN 1 ELSE 0 END) >= 36
ORDER BY pct_chiang_mai
