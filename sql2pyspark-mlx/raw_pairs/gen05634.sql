WITH latest AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY rating NULLS LAST, id) AS rn
        FROM suppliers
    ) t
    WHERE rn = 1
)
SELECT
    id,
    supplier_name,
    country,
    city,
    rating,
    rn,
    CASE WHEN city RLIKE '^(New York|Berlin)$' THEN 'CORE' ELSE 'OTHER_ZONE' END AS city_zone
FROM latest;
