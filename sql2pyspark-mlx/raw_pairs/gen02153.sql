WITH kept_keys AS (
    -- Set difference on keys
    SELECT id, city
    FROM suppliers
    EXCEPT ALL
    SELECT id, city
    FROM suppliers
    WHERE supplier_name LIKE 'Si%'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM suppliers s
    JOIN kept_keys k ON s.id = k.id AND s.city = k.city
)
SELECT
    id,
    supplier_name,
    city,
    rating,
    CASE WHEN city RLIKE '^(London|Tokyo)$' THEN 'GROUP_A' WHEN city RLIKE '^(Bangkok|Chiang Mai)$' THEN 'OTHER_ZONE' ELSE 'GROUP_B' END AS city_zone
FROM kept_rows
ORDER BY city_zone DESC, city ASC;
