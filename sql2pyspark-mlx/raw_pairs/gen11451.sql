WITH not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM suppliers s
    WHERE EXISTS (
            SELECT 1
            FROM products x
            WHERE x.supplier_id = s.id
              AND x.cost <= 600
        )
)
SELECT
    id,
    supplier_name,
    country,
    city,
    rating,
    CASE WHEN country RLIKE '^(UK|TH)$' THEN 'GROUP_B' ELSE 'GROUP_A' END AS country_zone
FROM not_flagged
ORDER BY country_zone DESC NULLS FIRST;
