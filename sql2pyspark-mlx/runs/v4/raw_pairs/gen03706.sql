WITH deduped AS (
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY city, country ORDER BY rating DESC NULLS FIRST) AS row_num
        FROM suppliers
    ) x
    WHERE row_num = 1
)
SELECT
    id,
    supplier_name,
    country,
    city,
    rating,
    row_num,
    CASE WHEN city RLIKE '^(London|Berlin)$' THEN 'GROUP_A' ELSE 'EXTENDED' END AS city_zone
FROM deduped;
