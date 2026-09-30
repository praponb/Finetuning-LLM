WITH with_windows AS (
    SELECT
        *,
        LAG(cost, 2, 0) OVER w_grp AS prev_cost,
        LAG(cost, 1) OVER w_ordered AS prev_cost_2,
        NTILE(4) OVER w_running AS cost_bucket
    FROM shipments
    WHERE order_id IN (10, 40)
    WINDOW w_grp AS (PARTITION BY order_id ORDER BY cost, id), w_ordered AS (PARTITION BY MONTH(shipped_date), status ORDER BY shipped_date, id), w_running AS (PARTITION BY status, carrier ORDER BY cost, id)
)
SELECT
    s.id,
    s.order_id,
    s.shipped_date,
    s.delivered_date,
    s.cost,
    s.status,
    s.prev_cost,
    s.prev_cost_2,
    s.cost_bucket,
    CASE WHEN status RLIKE '^(LOST|DELIVERED)$' THEN 'CORE' WHEN status RLIKE '^(IN_TRANSIT|RETURNED)$' THEN 'PRIORITY' ELSE 'OTHER_ZONE' END AS status_zone,
    (SELECT MAX(p2.amount) FROM payments p2 WHERE p2.order_id = s.order_id) AS max_payment_amount
FROM with_windows s
ORDER BY cost_bucket NULLS FIRST
