WITH filtered AS (
    SELECT *, CASE WHEN amount >= 1710 THEN 'High' WHEN amount >= 220 THEN 'Medium' ELSE 'Low' END AS amount_band
    FROM orders
    WHERE order_date >= DATE '2023-06-16'
      AND amount > 1150
),
attributed AS (
    SELECT
        s.*,
        sub.id AS subscription_id,
        sub.plan,
        sub.monthly_fee
    FROM filtered s
    LEFT JOIN subscriptions sub
        ON s.customer_id = sub.customer_id
       AND s.order_date >= sub.start_date
       AND s.order_date < sub.end_date + INTERVAL '30' DAY
),
kept AS (
    SELECT s.*
    FROM attributed s
    WHERE NOT EXISTS (
            SELECT 1
            FROM order_items ch
            WHERE ch.order_id = s.id
              AND ch.unit_price BETWEEN 210 AND 400
        )
),
kept_orders AS (
    SELECT s.*
    FROM kept s
    WHERE EXISTS (
            SELECT 1
            FROM shipments x
            WHERE x.order_id = s.id
              AND x.shipped_date <= '2025-05-01'
        )
),
eligible AS (
    SELECT s.*
    FROM kept_orders s
    WHERE NOT EXISTS (
            SELECT 1
            FROM shipments ch
            WHERE ch.order_id = s.id
              AND ch.shipped_date >= s.order_date
        )
),
deduped AS (
    SELECT
        id,
        discount,
        subscription_id,
        monthly_fee
    FROM eligible
    QUALIFY ROW_NUMBER() OVER (PARTITION BY plan, status ORDER BY discount DESC, id, subscription_id) = 1
),
with_last AS (
    SELECT s.*, latest.delivered_date
    FROM deduped s
    JOIN LATERAL (
            SELECT c.delivered_date
            FROM shipments c
            WHERE c.order_id = s.id
            ORDER BY c.cost DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
)
SELECT
    DATE_TRUNC('WEEK', delivered_date) AS week_start,
    subscription_id,
    COUNT(*) AS event_count,
    ROUND(AVG(monthly_fee), 2) AS avg_monthly_fee,
    AVG(discount) AS avg_discount,
    COUNT(*) AS cnt,
    MIN(discount) AS min_discount
FROM with_last
GROUP BY week_start, subscription_id
