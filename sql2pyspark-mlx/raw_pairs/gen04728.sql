WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        store_id,
        order_date,
        discount,
        shipping_fee,
        LEAST(amount, discount) AS least_amount_discount
    FROM orders
    WHERE order_date >= TO_DATE('2023-06-14')
      AND channel IS NOT NULL
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM base s
    WHERE NOT EXISTS (
            SELECT 1
            FROM shipments ch
            WHERE ch.order_id = s.id
              AND ch.carrier = 'UPS'
        )
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM not_flagged s
    WHERE EXISTS (
            SELECT 1
            FROM order_items x
            WHERE x.order_id = s.id
              AND x.unit_price > 380
        )
),
eligible_orders AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM eligible s
    WHERE NOT EXISTS (
            SELECT 1
            FROM shipments x
            WHERE x.order_id = s.id
              AND x.delivered_date <= s.order_date
        )
),
with_latest AS (
    -- Latest matching payment per row (LATERAL ... LIMIT 1)
    SELECT s.*, latest.status
    FROM eligible_orders s
    LEFT JOIN LATERAL (
            SELECT c.status
            FROM payments c
            WHERE c.order_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
totals AS (
    -- Multi-level aggregation
    SELECT
        CASE WHEN shipping_fee >= 36 THEN 'HIGH' ELSE 'LOW' END AS shipping_fee_tier,
        DATE_TRUNC('YEAR', order_date) AS year_start,
        SUM(least_amount_discount) FILTER (WHERE status = 'FAILED') AS failed_total_least_amount_discount,
        PERCENTILE_APPROX(shipping_fee, 0.95) AS shipping_fee_p95,
        PERCENTILE_APPROX(discount, 0.5) AS discount_p50
    FROM with_latest
    GROUP BY CASE WHEN shipping_fee >= 36 THEN 'HIGH' ELSE 'LOW' END, DATE_TRUNC('YEAR', order_date)
)
SELECT
    shipping_fee_tier,
    year_start,
    failed_total_least_amount_discount,
    shipping_fee_p95
FROM totals
ORDER BY shipping_fee_p95 NULLS LAST;
