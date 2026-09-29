WITH filtered_stores AS (
    SELECT *
    FROM stores
    WHERE city NOT IN ('Berlin', 'Bangkok', 'Chiang Mai') AND sqft <= 6468
),
stores_stats AS (
    SELECT YEAR(opened_date) AS opened_year, COUNT(CASE WHEN city = 'Chiang Mai' THEN 1 END) AS chiang_mai_count
    FROM filtered_stores
    GROUP BY YEAR(opened_date)
)
SELECT *
FROM stores_stats
