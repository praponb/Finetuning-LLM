WITH by_dimension AS (
    -- Join a dimension and aggregate
    SELECT
        p.category,
        p.brand,
        COUNT(*) AS num_rows,
        SUM(s.quantity) AS total_quantity
    FROM inventory s
    LEFT JOIN products p ON s.product_id = p.id
    GROUP BY GROUPING SETS ((p.category, p.brand), (p.category))
)
SELECT
    category,
    brand,
    CASE WHEN brand RLIKE '^(Umbrella|Globex)$' THEN 'OTHER_ZONE' WHEN brand RLIKE '^(Acme|Initech)$' THEN 'CORE' ELSE 'PRIORITY' END AS brand_zone
FROM by_dimension
