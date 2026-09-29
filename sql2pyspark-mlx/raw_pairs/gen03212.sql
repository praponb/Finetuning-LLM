WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        device,
        browser,
        started_at,
        country,
        CASE WHEN browser = 'Chrome' THEN 1 ELSE 0 END AS is_chrome,
        COALESCE(country, 'UNKNOWN') AS country_filled
    FROM sessions
    WHERE started_at >= TO_DATE('2023-07-10')
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM filtered s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events ch
            WHERE ch.session_id = s.id
              AND ch.event_type = 'page_view'
        )
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        MAX(is_chrome) OVER w_recent AS running_max_is_chrome,
        AVG(is_chrome) OVER w_recent AS running_avg_is_chrome,
        MIN(is_chrome) OVER w_recent AS running_min_is_chrome,
        SUM(is_chrome) OVER w_recent AS cumulative_is_chrome
    FROM eligible
    WHERE country NOT IN ('DE', 'SG', 'JP')
    WINDOW w_recent AS (PARTITION BY device ORDER BY is_chrome, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        device,
        is_chrome,
        DENSE_RANK() OVER (PARTITION BY country, browser ORDER BY running_min_is_chrome DESC) AS running_min_is_chrome_dense_rank,
        FIRST_VALUE(running_min_is_chrome, TRUE) OVER (PARTITION BY device ORDER BY cumulative_is_chrome, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_running_min_is_chrome
    FROM metrics
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        device,
        first_known_running_min_is_chrome
    FROM (
        SELECT *, RANK() OVER (PARTITION BY device ORDER BY is_chrome NULLS FIRST) AS rn
        FROM with_windows
    ) t
    WHERE rn = 1
),
eligible_sessions AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM latest s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events x
            WHERE x.session_id = s.id
              AND x.duration_sec = 216
        )
),
valid AS (
    -- Keep recent, valid rows
    SELECT *, FLOOR(first_known_running_min_is_chrome) AS first_known_running_min_is_chrome_whole
    FROM eligible_sessions
    WHERE device != 'mobile'
),
report AS (
    SELECT
        id,
        device,
        first_known_running_min_is_chrome,
        CASE WHEN device RLIKE '^(mobile|desktop)$' THEN 'CORE' WHEN device RLIKE '^(tablet)$' THEN 'GROUP_A' ELSE 'GROUP_B' END AS device_zone
    FROM valid
)
SELECT device, device_zone
FROM report
ORDER BY device NULLS LAST;
