WITH prev_events AS (
    SELECT *, LAG(started_at) OVER (PARTITION BY customer_id ORDER BY started_at, id) AS prev_started_at
    FROM sessions
),
flagged AS (
    SELECT *, CASE WHEN prev_started_at IS NULL OR started_at > prev_started_at + INTERVAL 90 DAYS THEN 1 ELSE 0 END AS is_new_burst
    FROM prev_events
),
islands AS (
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY customer_id ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM flagged
)
SELECT
    s.id,
    s.customer_id,
    s.device,
    s.browser,
    s.started_at,
    s.country,
    s.prev_started_at,
    s.island_id,
    CASE WHEN device RLIKE '^(tablet|mobile)$' THEN 'CORE' ELSE 'GROUP_B' END AS device_zone,
    (SELECT AVG(o2.discount) FROM orders o2 WHERE o2.customer_id = s.customer_id) AS avg_order_discount
FROM islands s
ORDER BY device_zone DESC;
