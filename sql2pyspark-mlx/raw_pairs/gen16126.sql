WITH with_dims AS (
    -- Attach customers attributes
    SELECT s.*, c.signup_date
    FROM sessions s
    LEFT JOIN customers c
        ON s.customer_id = c.id
    WHERE c.is_active
),
prev_events AS (
    -- Previous event time per key
    SELECT *, LAG(started_at) OVER (PARTITION BY country ORDER BY started_at, id) AS prev_started_at
    FROM with_dims
),
flagged AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_started_at IS NULL OR started_at > prev_started_at + INTERVAL 60 DAYS THEN 1 ELSE 0 END AS is_new_burst
    FROM prev_events
),
islands AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        customer_id,
        device,
        browser,
        started_at,
        country,
        SUM(is_new_burst) OVER (PARTITION BY country ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        customer_id,
        started_at,
        MIN(session_id) OVER w_ordered AS running_min_session_id,
        COUNT(session_id) OVER w_ordered AS running_count_session_id,
        AVG(session_id) OVER w_ordered AS running_avg_session_id,
        SUM(session_id) OVER w_ordered AS running_sum_session_id,
        AVG(session_id) OVER w_recent AS browser_avg_session_id
    FROM islands
    WINDOW w_ordered AS (PARTITION BY customer_id ORDER BY session_id, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_recent AS (PARTITION BY browser)
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM metrics s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events x
            WHERE x.session_id = s.id
              AND x.event_time >= s.started_at
        )
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        started_at,
        running_min_session_id,
        running_sum_session_id,
        browser_avg_session_id,
        COALESCE(running_min_session_id, 0) AS running_min_session_id_filled,
        running_min_session_id * browser_avg_session_id AS running_min_session_id_times_browser_avg_session_id
    FROM kept
    WHERE started_at > DATE '2023-08-18'
),
metrics_sessions AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(running_sum_session_id) OVER (ORDER BY browser_avg_session_id, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_running_sum_session_id,
        MIN(running_sum_session_id) OVER (ORDER BY browser_avg_session_id, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_running_sum_session_id,
        MAX(running_sum_session_id) OVER (ORDER BY browser_avg_session_id, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_running_sum_session_id,
        AVG(running_min_session_id_filled) OVER (ORDER BY running_min_session_id_times_browser_avg_session_id, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS running_min_session_id_filled_avg_2
    FROM filtered
)
-- Native SQL UNPIVOT (wide to long)
SELECT
    id,
    measure,
    metric_value
FROM (
    SELECT
        id,
        running_min_session_id_times_browser_avg_session_id,
        running_max_running_sum_session_id,
        running_min_running_sum_session_id
    FROM metrics_sessions
) src
UNPIVOT (
    metric_value FOR measure IN (running_min_session_id_times_browser_avg_session_id, running_max_running_sum_session_id, running_min_running_sum_session_id)
)
