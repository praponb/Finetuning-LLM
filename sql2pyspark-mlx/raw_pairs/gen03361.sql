WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, UPPER(browser) AS browser_upper
    FROM sessions
    WHERE started_at >= TO_DATE('2023-07-13')
      AND country IS NOT NULL
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN started_at - LAG(started_at) OVER (PARTITION BY device ORDER BY started_at, id) > INTERVAL '60' DAY THEN 1 ELSE 0 END AS is_new_session
    FROM filtered
),
islands AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        device,
        started_at,
        country,
        SUM(is_new_session) OVER (PARTITION BY device ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
)
-- Grouped metrics
SELECT
    device,
    ROUND(SUM(session_id) / NULLIF(COUNT(CASE WHEN device = 'tablet' THEN 1 END), 0), 2) AS session_id_per_tablet,
    MAX(session_id) FILTER (WHERE device = 'desktop') AS desktop_max_session_id,
    COUNT(CASE WHEN device = 'tablet' THEN 1 END) AS tablet_count
FROM islands
GROUP BY device
HAVING session_id_per_tablet < 1000000
ORDER BY device DESC
