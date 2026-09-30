WITH src AS (
    SELECT *
    FROM stores
    WHERE region = 'West'
),
stores_stats AS (
    SELECT DATE_FORMAT(opened_date, 'yyyy-MM') AS opened_month, MAX(opened_date) AS last_opened_date, MAX(sqft) - MIN(sqft) AS sqft_range
    FROM src
    GROUP BY DATE_FORMAT(opened_date, 'yyyy-MM')
)
SELECT *
FROM stores_stats
WHERE sqft_range > 35;
