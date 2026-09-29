SELECT
    QUARTER(opened_date) AS opened_quarter,
    region,
    ROUND((100 * SUM(CASE WHEN city = 'Chiang Mai' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_chiang_mai,
    COUNT(*) AS store_count
FROM stores
WHERE YEAR(opened_date) = 2023 OR city IN ('Bangkok', 'Singapore', 'New York')
GROUP BY QUARTER(opened_date), region
HAVING COUNT(*) < 5;
