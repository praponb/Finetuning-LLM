WITH by_dimension AS (
    SELECT
        d.country,
        d.city,
        COUNT(*) AS num_rows,
        ROUND(SUM(a.stock_qty), 2) AS total_stock_qty,
        GROUPING_ID() AS grouping_level
    FROM products a
    LEFT JOIN suppliers d ON a.supplier_id = d.id
    GROUP BY GROUPING SETS ((d.country, d.city), (d.country), ())
)
SELECT
    num_rows,
    total_stock_qty,
    grouping_level
FROM by_dimension
ORDER BY grouping_level DESC NULLS LAST, num_rows;
