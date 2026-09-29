WITH gaps AS (
    SELECT *, CASE WHEN LAG(paid_at) OVER (PARTITION BY order_id ORDER BY paid_at, id) IS NULL
                 OR paid_at > LAG(paid_at) OVER (PARTITION BY order_id ORDER BY paid_at, id) + INTERVAL '60' DAY THEN 1 ELSE 0 END AS is_new_burst
    FROM payments
),
islands AS (
    SELECT
        id,
        order_id,
        paid_at,
        SUM(is_new_burst) OVER (PARTITION BY order_id ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM gaps
)
SELECT
    s.id,
    s.order_id,
    s.paid_at,
    (SELECT MAX(x.cost) FROM shipments x WHERE x.order_id = s.order_id) AS max_shipment_cost
FROM islands s;
