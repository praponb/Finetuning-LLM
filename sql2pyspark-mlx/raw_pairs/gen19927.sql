WITH dim_totals AS (
    SELECT
        dim.category,
        COUNT(*) AS num_rows,
        MAX(a.quantity) AS max_quantity,
        GROUPING_ID() AS gid
    FROM inventory a
    LEFT JOIN products dim ON a.product_id = dim.id
    GROUP BY ROLLUP (dim.category)
)
SELECT
    category,
    num_rows,
    max_quantity,
    gid,
    CASE WHEN category RLIKE '^(Books|Grocery)$' THEN 'EXTENDED' ELSE 'CORE' END AS category_zone
FROM dim_totals
ORDER BY gid;
