SELECT
    segment,
    ROUND((100 * SUM(CASE WHEN city = 'Berlin' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_berlin,
    COUNT(*) AS row_count,
    ROUND((100 * SUM(CASE WHEN city = 'Tokyo' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_tokyo
FROM customers
GROUP BY segment
HAVING COUNT(CASE WHEN segment = 'Corporate' THEN 1 END) > 1
ORDER BY pct_tokyo DESC, segment
LIMIT 5
