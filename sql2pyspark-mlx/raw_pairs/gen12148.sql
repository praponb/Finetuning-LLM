WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        supplier_name,
        country,
        city,
        CONCAT(country, '/', supplier_name) AS country_supplier_name
    FROM suppliers
    WHERE city = 'Berlin'
),
with_latest AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.country_supplier_name,
        latest.stock_qty,
        latest.category
    FROM filtered s
    JOIN LATERAL (
            SELECT c.stock_qty, c.category
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.cost DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, SUBSTRING(category, 1, 2) AS category_prefix
    FROM with_latest
    WHERE stock_qty BETWEEN 139 AND 172
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM cleaned s
    WHERE EXISTS (
            SELECT 1
            FROM products x
            WHERE x.supplier_id = s.id
              AND x.supplier_id IN (12, 38)
        )
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        country_supplier_name,
        stock_qty,
        category_prefix,
        LEAD(stock_qty) OVER w_running AS next_stock_qty,
        LAG(stock_qty) OVER w_running AS prev_stock_qty,
        ROW_NUMBER() OVER w_running AS event_seq
    FROM eligible
    WINDOW w_running AS (PARTITION BY category ORDER BY stock_qty, id)
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, next_stock_qty * stock_qty AS next_stock_qty_times_stock_qty
    FROM enriched
    WHERE prev_stock_qty <= 180
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM valid s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products ch
            WHERE ch.supplier_id = s.id
              AND ch.category IN ('Electronics', 'Fashion', 'Grocery')
        )
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN next_stock_qty >= 354 THEN 'HIGH' ELSE 'LOW' END AS next_stock_qty_tier,
    APPROX_PERCENTILE(stock_qty, 0.5) AS stock_qty_p50,
    COUNT(DISTINCT id) AS distinct_ids,
    COUNT(*) AS cnt
FROM not_flagged
GROUP BY 1;
