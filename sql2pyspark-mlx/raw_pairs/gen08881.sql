WITH rollup_totals AS (
    SELECT
        p.category,
        p.brand,
        COUNT(*) AS rows_total,
        ROUND(SUM(s.quantity), 2) AS total_quantity
    FROM inventory s
    LEFT JOIN products p ON s.product_id = p.id
    GROUP BY p.category, p.brand
)
SELECT
    category,
    brand,
    rows_total,
    CASE WHEN brand RLIKE '^(Stark|Initech)$' THEN 'GROUP_A' ELSE 'GROUP_B' END AS brand_zone
FROM rollup_totals
ORDER BY brand DESC, category DESC
