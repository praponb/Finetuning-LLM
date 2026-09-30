WITH ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY amount DESC NULLS LAST, id) AS rn
        FROM payments
    ) x
    WHERE rn = 1
),
enriched AS (
    -- Attach orders attributes
    SELECT
        a.*,
        p.discount,
        p.shipping_fee
    FROM ranked a
    INNER JOIN orders p
        ON a.order_id = p.id
    WHERE p.shipping_fee > 21
),
with_dims AS (
    -- Attach orders attributes
    SELECT
        src.*,
        o.channel,
        o.status AS order_status,
        o.shipping_fee AS order_shipping_fee
    FROM enriched src
    LEFT JOIN orders o
        ON src.order_id = o.id
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(shipping_fee) OVER (PARTITION BY TO_DATE(paid_at), order_status ORDER BY rn, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_shipping_fee,
        PERCENT_RANK() OVER (PARTITION BY order_id ORDER BY order_shipping_fee DESC NULLS LAST) AS order_shipping_fee_percent_rank,
        FIRST_VALUE(channel) IGNORE NULLS OVER (PARTITION BY method ORDER BY shipping_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_channel
    FROM with_dims
)
-- Multi-level aggregation
SELECT
    CASE WHEN discount >= 0.4 THEN 'HIGH' ELSE 'LOW' END AS discount_tier,
    CASE WHEN running_shipping_fee >= 179 THEN 'LARGE' ELSE 'SMALL' END AS running_shipping_fee_tier,
    SUM(order_shipping_fee) AS total_order_shipping_fee,
    ROUND(SUM(order_shipping_fee) / NULLIF(COUNT(CASE WHEN status = 'REFUNDED' THEN 1 END), 0), 2) AS order_shipping_fee_per_refunded,
    SUM(shipping_fee) AS total_shipping_fee
FROM with_windows
GROUP BY CASE WHEN discount >= 0.4 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN running_shipping_fee >= 179 THEN 'LARGE' ELSE 'SMALL' END;
