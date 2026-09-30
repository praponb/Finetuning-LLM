WITH remaining_keys AS (
    -- Set intersection on keys
    SELECT id, category
    FROM products
    INTERSECT
    SELECT id, category
    FROM products
    WHERE product_name LIKE '%Mat%'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM products s
    INNER JOIN remaining_keys k ON s.id = k.id AND s.category = k.category
)
-- Multi-level aggregation
SELECT
    CASE WHEN stock_qty >= 334 THEN 'HIGH' ELSE 'LOW' END AS stock_qty_tier,
    COUNT(*) AS num_rows,
    SUM(stock_qty) AS total_stock_qty,
    MAX(cost) AS max_cost,
    COUNT(*) FILTER (WHERE category = 'Sports') AS sports_rows,
    PERCENTILE_APPROX(price, ARRAY(0.25, 0.5, 0.75)) AS price_quartiles,
    COUNT(DISTINCT id) AS distinct_ids
FROM reconciled
GROUP BY CASE WHEN stock_qty >= 334 THEN 'HIGH' ELSE 'LOW' END
ORDER BY sports_rows DESC NULLS LAST
