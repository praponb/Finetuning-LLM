WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        customer_id,
        store_id,
        order_date,
        status,
        channel,
        shipping_fee,
        UPPER(channel) AS channel_upper
    FROM orders
    WHERE channel <> 'partner'
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM valid
    QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY shipping_fee, id) = 1
),
with_latest AS (
    -- Latest matching payment per row (LATERAL ... LIMIT 1)
    SELECT s.*, x.amount
    FROM ranked s
    LEFT JOIN LATERAL (
            SELECT c.amount
            FROM payments c
            WHERE c.order_id = s.id
              AND c.paid_at <= s.order_date
            ORDER BY c.paid_at DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM with_latest s
    WHERE EXISTS (
            SELECT 1
            FROM shipments x
            WHERE x.order_id = s.id
              AND x.delivered_date < s.order_date
        )
),
latest AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        customer_id,
        order_date,
        status,
        shipping_fee,
        amount
    FROM kept
    QUALIFY ROW_NUMBER() OVER (PARTITION BY store_id, customer_id ORDER BY shipping_fee NULLS FIRST, id) = 1
)
-- Join a dimension and aggregate
SELECT
    p.city,
    e.status,
    COUNT(*) AS rows_total,
    AVG(e.shipping_fee) AS avg_shipping_fee,
    SUM(e.amount) AS total_amount,
    GROUPING_ID() AS level_id
FROM latest e
INNER JOIN customers p ON e.customer_id = p.id
GROUP BY ROLLUP (p.city, e.status)
