WITH kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM products s
    WHERE NOT EXISTS (
            SELECT 1
            FROM order_items c
            WHERE c.product_id = s.id
              AND c.unit_price BETWEEN 220 AND 310
        )
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(5) OVER (PARTITION BY supplier_id, category ORDER BY price DESC, id) AS price_bucket,
        LAST_VALUE(stock_qty, TRUE) OVER (ORDER BY price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_stock_qty,
        NTILE(10) OVER w_running AS cost_bucket
    FROM kept
    WINDOW w_running AS (ORDER BY cost, id)
),
matched_keys AS (
    -- Set intersection on keys
    SELECT id
    FROM scored
    INTERSECT ALL
    SELECT id
    FROM products
    WHERE cost BETWEEN 310 AND 430
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM scored s
    INNER JOIN matched_keys k ON s.id = k.id
),
scored_products AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY category ORDER BY last_known_stock_qty DESC) AS last_known_stock_qty_dense_rank,
        ROUND(price_bucket / SUM(price_bucket) OVER (PARTITION BY brand), 4) AS price_bucket_share,
        PERCENT_RANK() OVER (ORDER BY price DESC NULLS FIRST) AS price_percent_rank,
        PERCENT_RANK() OVER (ORDER BY stock_qty DESC NULLS LAST) AS stock_qty_percent_rank
    FROM reconciled
),
matched_keys_2 AS (
    -- Set difference on keys
    SELECT category, supplier_id
    FROM scored_products
    EXCEPT ALL
    SELECT category, supplier_id
    FROM products
    WHERE product_name IS NOT NULL
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM scored_products s
    INNER JOIN matched_keys_2 k ON s.category = k.category AND s.supplier_id = k.supplier_id
)
-- Grouped metrics
SELECT
    category,
    CASE WHEN price >= 910 THEN 'PREMIUM' ELSE 'STANDARD' END AS price_tier,
    ROUND(SUM(price_percent_rank) / NULLIF(COUNT(CASE WHEN brand = 'Globex' THEN 1 END), 0), 2) AS price_percent_rank_per_globex,
    COUNT_IF(category = 'Fashion') AS fashion_events,
    MAX(last_known_stock_qty_dense_rank) FILTER (WHERE category = 'Home') AS home_max_last_known_stock_qty_dense_rank,
    MAX(price_bucket_share) FILTER (WHERE category = 'Sports') AS sports_max_price_bucket_share
FROM verified
GROUP BY ROLLUP (category, CASE WHEN price >= 910 THEN 'PREMIUM' ELSE 'STANDARD' END)
HAVING MAX(last_known_stock_qty_dense_rank) FILTER (WHERE category = 'Home') < 0
