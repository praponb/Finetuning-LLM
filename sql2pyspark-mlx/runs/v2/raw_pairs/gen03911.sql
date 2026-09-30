WITH base AS (
    SELECT *
    FROM suppliers
    WHERE country IN ('JP', 'SG')
),
summary AS (
    SELECT CASE WHEN rating >= 3.6 THEN 'high' WHEN rating >= 2.4 THEN 'mid' ELSE 'low' END AS rating_bucket, country, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(country))) AS country_values
    FROM base
    GROUP BY CASE WHEN rating >= 3.6 THEN 'high' WHEN rating >= 2.4 THEN 'mid' ELSE 'low' END, country
)
SELECT *
FROM summary
ORDER BY rating_bucket DESC, country
