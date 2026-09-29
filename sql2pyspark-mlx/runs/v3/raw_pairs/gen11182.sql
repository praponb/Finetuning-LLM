WITH recent_stores AS (
    SELECT *
    FROM stores
    WHERE region NOT IN ('Central', 'North') AND sqft < 9013
),
summary AS (
    SELECT MONTH(opened_date) AS opened_month, COUNT(CASE WHEN city = 'Tokyo' THEN 1 END) AS tokyo_count, SUM(CASE WHEN region = 'West' THEN 1 ELSE 0 END) AS west_count
    FROM recent_stores
    GROUP BY MONTH(opened_date)
)
SELECT opened_month, west_count
FROM summary
WHERE tokyo_count >= 10
ORDER BY west_count DESC, opened_month
LIMIT 10;
