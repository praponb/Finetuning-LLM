WITH gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN started_at - LAG(started_at) OVER (PARTITION BY customer_id ORDER BY started_at, id) > INTERVAL 90 DAYS THEN 1 ELSE 0 END AS is_new_burst
    FROM sessions
),
islands AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        customer_id,
        device,
        started_at,
        country,
        SUM(is_new_burst) OVER (PARTITION BY customer_id ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM gaps
)
SELECT
    s.id,
    s.customer_id,
    s.country,
    s.session_id,
    CASE WHEN country RLIKE '^(TH|SG)$' THEN 'EXTENDED' WHEN country RLIKE '^(US|JP)$' THEN 'PRIORITY' ELSE 'CORE' END AS country_zone,
    (SELECT MIN(x.amount) FROM orders x WHERE x.customer_id = s.customer_id) AS min_order_amount
FROM islands s
ORDER BY country DESC, min_order_amount DESC;
