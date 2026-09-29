WITH filtered_stores AS (
    SELECT *
    FROM stores
    WHERE city NOT IN ('Chiang Mai', 'Tokyo') AND YEAR(opened_date) >= 2025
),
summary AS (
    SELECT city, ROUND(AVG(sqft), 2) AS avg_sqft
    FROM filtered_stores
    GROUP BY city
)
SELECT city, avg_sqft
FROM summary
ORDER BY avg_sqft DESC, city DESC;
