WITH matched_keys AS (
    SELECT city, country
    FROM customers
    EXCEPT DISTINCT
    SELECT city, country
    FROM customers
    WHERE city IN ('London', 'Chiang Mai', 'Singapore')
),
kept_rows AS (
    SELECT s.*
    FROM customers s
    INNER JOIN matched_keys k ON s.city = k.city AND s.country = k.country
)
SELECT
    CASE WHEN age >= 63 THEN 'HIGH' ELSE 'LOW' END AS age_tier,
    ROUND(SUM(age) / NULLIF(COUNT(CASE WHEN country = 'DE' THEN 1 END), 0), 2) AS age_per_de,
    SORT_ARRAY(COLLECT_SET(city)) AS city_list
FROM kept_rows
GROUP BY age_tier
