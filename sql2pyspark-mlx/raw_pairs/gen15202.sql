WITH with_gaps AS (
    SELECT *, CASE WHEN LAG(paid_at) OVER (PARTITION BY method ORDER BY paid_at, id) IS NULL
                 OR paid_at > LAG(paid_at) OVER (PARTITION BY method ORDER BY paid_at, id) + INTERVAL 90 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM payments
),
sessions_payments AS (
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY method ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM with_gaps
)
SELECT
    s.id,
    s.order_id,
    s.method,
    s.amount,
    s.paid_at,
    s.session_start_flag,
    s.session_id,
    (SELECT MIN(t2.cost) FROM shipments t2 WHERE t2.order_id = s.order_id) AS min_shipment_cost
FROM sessions_payments s;
