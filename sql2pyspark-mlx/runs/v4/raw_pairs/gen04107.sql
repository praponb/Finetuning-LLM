WITH gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(started_at) OVER (PARTITION BY customer_id ORDER BY started_at, id) IS NULL
                 OR started_at > LAG(started_at) OVER (PARTITION BY customer_id ORDER BY started_at, id) + INTERVAL '14' DAY THEN 1 ELSE 0 END AS is_new_session
    FROM sessions
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY customer_id ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM gaps
)
SELECT
    id,
    customer_id,
    device,
    browser,
    started_at,
    is_new_session,
    island_id,
    CASE WHEN browser RLIKE '^(Firefox|Chrome)$' THEN 'CORE' ELSE 'PRIORITY' END AS browser_zone
FROM islands
ORDER BY island_id;
