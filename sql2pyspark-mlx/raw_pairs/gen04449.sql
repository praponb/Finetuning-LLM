WITH flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN started_at - LAG(started_at) OVER (PARTITION BY browser ORDER BY started_at, id) > INTERVAL 7 DAYS THEN 1 ELSE 0 END AS is_new_burst
    FROM sessions
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY browser ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
)
SELECT
    s.id,
    s.customer_id,
    s.browser,
    s.started_at,
    s.country,
    s.session_no,
    CASE WHEN country RLIKE '^(SG|DE)$' THEN 'OTHER_ZONE' ELSE 'GROUP_B' END AS country_zone,
    (SELECT MAX(t2.monthly_fee) FROM subscriptions t2 WHERE t2.customer_id = s.customer_id) AS max_subscription_monthly_fee
FROM islands s
ORDER BY id
