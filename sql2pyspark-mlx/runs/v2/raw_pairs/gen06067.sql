SELECT
    QUARTER(opened_date) opened_quarter,
    SUM(sqft) total_sqft,
    AVG(sqft) avg_sqft,
    COUNT(CASE WHEN city = 'Singapore' THEN 1 END) singapore_count
FROM stores
WHERE MONTH(opened_date) IN (2, 3, 12) AND (region IN ('East', 'Central', 'South') OR region = 'South')
GROUP BY QUARTER(opened_date)
ORDER BY total_sqft DESC, avg_sqft DESC, opened_quarter
LIMIT 50;
