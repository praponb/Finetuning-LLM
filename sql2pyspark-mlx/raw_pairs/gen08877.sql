WITH prev_events AS (
    -- Previous event time per key
    SELECT *, LAG(paid_at) OVER (PARTITION BY method ORDER BY paid_at, id) AS prev_paid_at
    FROM payments
),
gaps AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_paid_at IS NULL OR paid_at > prev_paid_at + INTERVAL '60' DAY THEN 1 ELSE 0 END AS new_group_flag
    FROM prev_events
),
sessions_payments AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY method ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM gaps
)
SELECT
    s.id,
    s.order_id,
    s.method,
    s.paid_at,
    s.status,
    s.prev_paid_at,
    s.new_group_flag,
    CASE WHEN method RLIKE '^(paypal|card)$' THEN 'CORE' ELSE 'OTHER_ZONE' END AS method_zone,
    (SELECT SUM(t2.cost) FROM shipments t2 WHERE t2.order_id = s.order_id) AS sum_shipment_cost
FROM sessions_payments s
ORDER BY method_zone DESC, sum_shipment_cost
