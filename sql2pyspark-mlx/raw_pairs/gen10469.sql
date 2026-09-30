WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, LENGTH(channel) AS channel_length
    FROM orders
    WHERE order_date > DATE '2023-06-24'
      AND status IN ('SHIPPED', 'RETURNED')
),
with_latest AS (
    -- Latest matching shipment per row (LATERAL ... LIMIT 1)
    SELECT s.*, x.shipped_date
    FROM filtered s
    LEFT JOIN LATERAL (
            SELECT c.shipped_date
            FROM shipments c
            WHERE c.order_id = s.id
            ORDER BY c.shipped_date DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_latest s
    WHERE NOT EXISTS (
            SELECT 1
            FROM shipments ch
            WHERE ch.order_id = s.id
              AND ch.cost BETWEEN 64 AND 81
        )
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM kept s
    WHERE NOT EXISTS (
            SELECT 1
            FROM order_items c
            WHERE c.order_id = s.id
              AND c.product_id IN (14, 15, 24, 29)
        )
),
by_type AS (
    -- Native SQL PIVOT
    SELECT *
    FROM (
        SELECT
            store_id,
            customer_id,
            channel,
            amount
        FROM not_flagged
    ) src
    PIVOT (
        AVG(amount) AS value_sum,
        COUNT(1) AS cnt
        FOR channel IN ('web' AS web, 'mobile' AS mobile)
    )
)
SELECT
    s.store_id,
    s.customer_id,
    s.web_value_sum,
    s.web_cnt,
    s.mobile_value_sum,
    (SELECT MIN(x.discount) FROM orders x WHERE x.store_id = s.store_id) AS min_order_discount
FROM by_type s
