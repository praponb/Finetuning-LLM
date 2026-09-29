WITH kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM orders s
    WHERE EXISTS (
            SELECT 1
            FROM shipments x
            WHERE x.order_id = s.id
              AND x.shipped_date <= s.order_date
        )
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        store_id,
        channel,
        amount,
        discount,
        shipping_fee,
        DATE_SUB(order_date, 54) AS order_date_minus_54d,
        COALESCE(channel, 'N/A') AS resolved_channel
    FROM kept
    WHERE order_date > TO_DATE('2023-10-31')
      AND customer_id IS NOT NULL
      AND customer_id IN (2, 13, 17)
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM filtered s
    WHERE EXISTS (
            SELECT 1
            FROM payments ch
            WHERE ch.order_id = s.id
              AND ch.status IN ('REFUNDED', 'FAILED')
        )
)
-- Ranking and gap features
SELECT
    *,
    LAST_VALUE(amount, TRUE) OVER (PARTITION BY channel ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_amount,
    LEAD(shipping_fee, 2) OVER (ORDER BY order_date_minus_54d, id) AS next_shipping_fee,
    MAX(shipping_fee) OVER (PARTITION BY channel) AS channel_max_shipping_fee,
    LAST_VALUE(shipping_fee) IGNORE NULLS OVER (PARTITION BY channel, store_id ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_shipping_fee,
    LEAD(shipping_fee, 2) OVER (PARTITION BY channel ORDER BY order_date_minus_54d, id) AS next_shipping_fee_2
FROM not_flagged;
