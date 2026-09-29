WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, DATE_SUB(sale_date, 61) AS sale_date_minus_61d
    FROM store_sales
    WHERE sale_date > '2023-05-01'
)
-- Join a dimension and aggregate
SELECT
    p.brand,
    COUNT(*) AS rows_total,
    MIN(s.units) AS min_units,
    ROUND(SUM(s.revenue), 2) AS total_revenue,
    GROUPING_ID() AS grouping_level
FROM cleaned s
INNER JOIN products p ON s.product_id = p.id
GROUP BY ROLLUP (p.brand)
ORDER BY brand DESC NULLS FIRST
