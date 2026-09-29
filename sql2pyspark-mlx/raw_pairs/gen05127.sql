WITH ranked AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        customer_id,
        store_id,
        order_date,
        status,
        amount,
        shipping_fee
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date DESC, id) AS dedup_rank
        FROM orders
    ) x
    WHERE dedup_rank = 1
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM ranked s
    WHERE NOT EXISTS (
            SELECT 1
            FROM order_items ch
            WHERE ch.order_id = s.id
              AND ch.quantity < 5
        )
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        status,
        amount,
        shipping_fee,
        CASE WHEN status = 'SHIPPED' THEN 1 ELSE 0 END AS is_shipped
    FROM kept
    WHERE order_date >= DATE '2023-05-20'
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY is_shipped, id) AS latest_rank
    FROM base
    QUALIFY latest_rank = 1
)
-- Grouped metrics
SELECT
    CASE WHEN is_shipped >= 67 THEN 'HIGH' ELSE 'LOW' END AS is_shipped_tier,
    SUM(CASE WHEN status = 'PENDING' THEN amount ELSE 0 END) AS pending_amount,
    COUNT(CASE WHEN status = 'PENDING' THEN 1 END) AS pending_count,
    APPROX_PERCENTILE(is_shipped, 0.95) AS is_shipped_p95,
    ROUND(SUM(shipping_fee) / NULLIF(COUNT(CASE WHEN status = 'PENDING' THEN 1 END), 0), 2) AS shipping_fee_per_pending,
    PERCENTILE_APPROX(is_shipped, 0.95) AS is_shipped_p95_2
FROM latest
GROUP BY CASE WHEN is_shipped >= 67 THEN 'HIGH' ELSE 'LOW' END
ORDER BY pending_amount DESC NULLS LAST
