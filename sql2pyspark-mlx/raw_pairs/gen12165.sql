WITH latest AS (
    -- Keep one row per key (latest first)
    SELECT *, RANK() OVER (PARTITION BY city, country ORDER BY rating DESC NULLS LAST) AS rn
    FROM suppliers
    QUALIFY rn = 1
)
SELECT
    id,
    city,
    rating,
    rn,
    CASE WHEN city RLIKE '^(Bangkok|Tokyo)$' THEN 'CORE' WHEN city RLIKE '^(Chiang Mai|New York)$' THEN 'GROUP_B' ELSE 'EXTENDED' END AS city_zone
FROM latest;
