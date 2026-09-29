WITH base AS (
    SELECT *
    FROM stores
    WHERE store_name LIKE '%Down%' AND opened_date BETWEEN '2024-02-13' AND '2024-05-17' AND LOWER(TRIM(store_name)) = 'riverside'
),
agg AS (
    SELECT DATE_TRUNC('MONTH', opened_date) AS opened_month, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(city))) AS city_values
    FROM base
    GROUP BY DATE_TRUNC('MONTH', opened_date)
)
SELECT *
FROM agg
ORDER BY city_values, opened_month
LIMIT 10;
