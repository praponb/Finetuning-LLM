WITH recent_stores AS (
    SELECT *
    FROM stores
    WHERE NOT (store_name LIKE 'O%' AND sqft >= 10776 AND region IN ('West', 'South', 'Central'))
),
agg AS (
    SELECT DATE_FORMAT(opened_date, 'yyyy-MM') AS opened_month, SUM(CASE WHEN region = 'West' THEN 1 ELSE 0 END) AS west_count
    FROM recent_stores
    GROUP BY DATE_FORMAT(opened_date, 'yyyy-MM')
)
SELECT opened_month, west_count
FROM agg
WHERE west_count < 35
