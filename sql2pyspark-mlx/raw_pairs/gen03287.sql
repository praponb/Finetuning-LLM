WITH kept_keys AS (
    SELECT city, id
    FROM stores
    EXCEPT ALL
    SELECT city, id
    FROM stores
    WHERE store_name IS NOT NULL
),
verified AS (
    SELECT s.*
    FROM stores s
    INNER JOIN kept_keys k ON s.city = k.city AND s.id = k.id
)
SELECT
    id,
    store_name,
    city,
    region,
    opened_date,
    sqft,
    CASE WHEN region RLIKE '^(East|North)$' THEN 'GROUP_A' ELSE 'EXTENDED' END AS region_zone
FROM verified
ORDER BY sqft DESC NULLS FIRST;
