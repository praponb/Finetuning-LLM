WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        TRIM(device) AS clean_device,
        CASE WHEN browser = 'Firefox' THEN 1 ELSE 0 END AS is_firefox
    FROM sessions
    WHERE started_at >= TO_DATE('2023-04-14')
      AND customer_id IS NOT NULL
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(started_at) OVER (PARTITION BY browser ORDER BY started_at, id) IS NULL
                 OR started_at > LAG(started_at) OVER (PARTITION BY browser ORDER BY started_at, id) + INTERVAL '30' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM filtered
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY browser ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM with_gaps
),
with_last AS (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.customer_id,
        s.device,
        s.browser,
        s.started_at,
        s.country,
        s.is_firefox,
        s.session_id,
        top1.page
    FROM sessionized s
    JOIN LATERAL (
            SELECT c.page
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.event_time DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY device ORDER BY started_at DESC NULLS LAST, id) AS row_num
    FROM with_last
    QUALIFY row_num = 1
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM ranked s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events c
            WHERE c.session_id = s.id
              AND c.event_time <= s.started_at
        )
)
-- Multi-level aggregation
SELECT
    CASE WHEN session_id >= 6 THEN 'HIGH' ELSE 'LOW' END AS session_id_tier,
    MIN(is_firefox) AS min_is_firefox,
    PERCENTILE_APPROX(is_firefox, 0.5) AS is_firefox_p50,
    MAX(started_at) AS ended_at,
    COUNT(*) FILTER (WHERE page = '/checkout') AS checkout_rows
FROM eligible
GROUP BY CASE WHEN session_id >= 6 THEN 'HIGH' ELSE 'LOW' END
ORDER BY is_firefox_p50 DESC NULLS LAST
