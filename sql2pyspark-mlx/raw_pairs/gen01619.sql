WITH flagged AS (
    SELECT *, CASE WHEN LAG(paid_at) OVER (PARTITION BY order_id ORDER BY paid_at, id) IS NULL
                 OR paid_at > LAG(paid_at) OVER (PARTITION BY order_id ORDER BY paid_at, id) + INTERVAL '30' DAY THEN 1 ELSE 0 END AS new_group_flag
    FROM payments
),
islands AS (
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY order_id ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
)
SELECT
    s.id,
    s.order_id,
    s.method,
    s.amount,
    s.session_no,
    (SELECT AVG(t2.cost) FROM shipments t2 WHERE t2.order_id = s.order_id) AS avg_shipment_cost
FROM islands s
