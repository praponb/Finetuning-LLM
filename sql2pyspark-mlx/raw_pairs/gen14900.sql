WITH kept AS (
    SELECT s.*
    FROM suppliers s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products c
            WHERE c.supplier_id = s.id
              AND c.cost >= 410
        )
)
SELECT
    id,
    supplier_name,
    country,
    city,
    rating,
    CASE WHEN country RLIKE '^(DE|JP)$' THEN 'PRIORITY' WHEN country RLIKE '^(TH|US)$' THEN 'EXTENDED' ELSE 'CORE' END AS country_zone
FROM kept
