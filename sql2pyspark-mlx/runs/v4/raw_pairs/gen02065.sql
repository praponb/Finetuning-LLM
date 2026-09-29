WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, ADD_MONTHS(launch_date, 6) launch_date_plus_months
    FROM products
    WHERE launch_date > DATE '2023-09-10'
      AND brand IS NOT NULL
),
latest_match AS (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        top1.quantity,
        top1.inventory_last_restock
    FROM cleaned s
    JOIN LATERAL (
            SELECT c.quantity, c.last_restock AS inventory_last_restock
            FROM inventory c
            WHERE c.product_id = s.id
            ORDER BY c.quantity DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM latest_match s
    WHERE EXISTS (
            SELECT 1
            FROM order_items ch
            WHERE ch.product_id = s.id
              AND ch.order_id IN (34, 40)
        )
),
kept_products AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM kept s
    WHERE EXISTS (
            SELECT 1
            FROM inventory ch
            WHERE ch.product_id = s.id
              AND ch.last_restock >= s.launch_date_plus_months
        )
),
enriched AS (
    -- Attach suppliers attributes
    SELECT src.*, p.city
    FROM kept_products src
    INNER JOIN suppliers p
        ON src.supplier_id = p.id
),
summary AS (
    -- Multi-level aggregation
    SELECT
        CASE WHEN stock_qty >= 366 THEN 'LARGE' ELSE 'SMALL' END AS stock_qty_tier,
        SUM(CASE WHEN category = 'Sports' THEN cost ELSE 0 END) AS sports_cost,
        COUNT(CASE WHEN city = 'Tokyo' THEN 1 END) AS tokyo_count,
        SUM(CASE WHEN city = 'London' THEN stock_qty ELSE 0 END) AS london_stock_qty,
        COUNT(DISTINCT id) AS distinct_ids
    FROM enriched
    GROUP BY 1
)
SELECT stock_qty_tier, sports_cost
FROM summary
ORDER BY sports_cost DESC NULLS LAST
