WITH with_latest AS (
    -- Latest matching store_sale per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.store_name,
        s.city,
        s.sqft,
        x.revenue,
        x.store_sale_units
    FROM stores s
    LEFT JOIN LATERAL (
            SELECT c.revenue, c.units AS store_sale_units
            FROM store_sales c
            WHERE c.store_id = s.id
            ORDER BY c.sale_date DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        store_name,
        sqft
    FROM with_latest
    QUALIFY ROW_NUMBER() OVER (PARTITION BY city ORDER BY sqft NULLS LAST, id) = 1
),
with_latest_stores AS (
    -- Latest matching order per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.store_name,
        s.sqft,
        top1.order_status
    FROM latest s
    LEFT JOIN LATERAL (
            SELECT c.status AS order_status
            FROM orders c
            WHERE c.store_id = s.id
            ORDER BY c.discount DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
with_last AS (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.store_name,
        s.order_status,
        x.last_restock
    FROM with_latest_stores s
    LEFT JOIN LATERAL (
            SELECT c.last_restock
            FROM inventory c
            WHERE c.store_id = s.id
            ORDER BY c.quantity DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM with_last s
    WHERE EXISTS (
            SELECT 1
            FROM store_sales ch
            WHERE ch.store_id = s.id
              AND ch.product_id IN (1, 6, 8)
        )
)
-- Grouped metrics
SELECT
    DATE_TRUNC('MONTH', last_restock) AS activity_month,
    order_status,
    COUNT_IF(order_status = 'SHIPPED') AS shipped_events,
    COUNT(CASE WHEN order_status = 'COMPLETED' THEN 1 END) AS completed_count,
    COUNT(CASE WHEN order_status = 'CANCELLED' THEN 1 END) AS cancelled_count,
    RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', last_restock) ORDER BY COUNT(CASE WHEN order_status = 'COMPLETED' THEN 1 END) DESC) AS completed_count_rank
FROM kept
GROUP BY 1, 2
