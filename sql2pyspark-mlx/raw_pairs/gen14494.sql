WITH dim_totals AS (
    SELECT
        dim.category,
        COUNT(*) AS rows_total,
        MIN(e.units) AS min_units,
        MIN(e.revenue) AS min_revenue
    FROM store_sales e
    JOIN products dim ON e.product_id = dim.id
    GROUP BY ROLLUP (dim.category)
)
SELECT min_units
FROM dim_totals;
