WITH src AS (
    SELECT *, LENGTH(city) city_length
    FROM stores
    WHERE YEAR(opened_date) >= 2023 AND opened_date BETWEEN '2023-06-01' AND '2023-07-22'
),
summary AS (
    SELECT
        MONTH(opened_date) opened_month,
        CASE WHEN city_length >= 78 THEN 'high' WHEN city_length >= 16 THEN 'mid' ELSE 'low' END city_length_bucket,
        ROUND((100 * SUM(CASE WHEN region = 'East' THEN 1 ELSE 0 END)) / COUNT(*), 1) pct_east,
        SORT_ARRAY(COLLECT_SET(city)) city_list
    FROM src
    GROUP BY MONTH(opened_date), CASE WHEN city_length >= 78 THEN 'high' WHEN city_length >= 16 THEN 'mid' ELSE 'low' END
)
SELECT *
FROM summary
ORDER BY 1 DESC;
