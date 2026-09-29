WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, DATEDIFF(CURRENT_DATE(), started_at) AS days_since_started_at
    FROM sessions
    WHERE device = 'desktop'
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN started_at - LAG(started_at) OVER (PARTITION BY country ORDER BY started_at, id) > INTERVAL 7 DAYS THEN 1 ELSE 0 END AS is_new_session
    FROM cleaned
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY country ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM with_gaps
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(island_id, 1) OVER (PARTITION BY customer_id ORDER BY started_at, id) AS prev_island_id,
        SUM(days_since_started_at) OVER (PARTITION BY country ORDER BY is_new_session, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_days_since_started_at,
        PERCENT_RANK() OVER (PARTITION BY browser ORDER BY is_new_session DESC NULLS LAST) AS is_new_session_percent_rank,
        LAST_VALUE(browser, TRUE) OVER w_ordered AS last_known_browser,
        NTILE(3) OVER (PARTITION BY customer_id ORDER BY days_since_started_at DESC, id) AS days_since_started_at_bucket
    FROM islands
    WINDOW w_ordered AS (PARTITION BY customer_id ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        device,
        browser,
        running_days_since_started_at,
        is_new_session_percent_rank,
        last_known_browser,
        SUM(is_new_session) OVER (PARTITION BY country ORDER BY is_new_session, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_sum_is_new_session,
        AVG(is_new_session) OVER (PARTITION BY country ORDER BY is_new_session, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_avg_is_new_session,
        MIN(is_new_session) OVER (PARTITION BY country ORDER BY is_new_session, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_min_is_new_session,
        FIRST_VALUE(browser) IGNORE NULLS OVER (PARTITION BY TO_DATE(started_at) ORDER BY days_since_started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_browser
    FROM with_windows
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        device,
        browser,
        running_days_since_started_at,
        is_new_session_percent_rank,
        last4_sum_is_new_session,
        last4_avg_is_new_session,
        CASE WHEN device IN ('mobile', 'tablet') THEN 'Group A' ELSE 'Other' END AS device_group
    FROM metrics
    WHERE last4_sum_is_new_session > 1.1
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        NTILE(10) OVER (PARTITION BY browser ORDER BY last4_avg_is_new_session DESC, id) AS last4_avg_is_new_session_bucket,
        CUME_DIST() OVER (PARTITION BY device ORDER BY last4_avg_is_new_session DESC) AS last4_avg_is_new_session_cume_dist
    FROM filtered
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM scored s
    WHERE EXISTS (
            SELECT 1
            FROM web_events x
            WHERE x.session_id = s.id
              AND x.event_time < '2024-03-01'
        )
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN last4_avg_is_new_session_bucket >= 8 THEN 'HIGH' ELSE 'LOW' END AS last4_avg_is_new_session_bucket_tier,
    device,
    CASE WHEN last4_sum_is_new_session >= 3.9 THEN 'PREMIUM' ELSE 'STANDARD' END AS last4_sum_is_new_session_tier,
    COUNT(*) FILTER (WHERE browser = 'Firefox') AS firefox_rows,
    AVG(is_new_session_percent_rank) FILTER (WHERE device = 'mobile') AS mobile_avg_is_new_session_percent_rank,
    COUNT(*) AS cnt,
    MAX(is_new_session_percent_rank) FILTER (WHERE device = 'mobile') AS mobile_max_is_new_session_percent_rank,
    SUM(CASE WHEN browser = 'Chrome' THEN last4_avg_is_new_session_bucket ELSE 0 END) AS chrome_last4_avg_is_new_session_bucket,
    GROUPING(CASE WHEN last4_avg_is_new_session_bucket >= 8 THEN 'HIGH' ELSE 'LOW' END) AS grouping_id
FROM kept
GROUP BY GROUPING SETS ((CASE WHEN last4_avg_is_new_session_bucket >= 8 THEN 'HIGH' ELSE 'LOW' END, device, CASE WHEN last4_sum_is_new_session >= 3.9 THEN 'PREMIUM' ELSE 'STANDARD' END), (CASE WHEN last4_avg_is_new_session_bucket >= 8 THEN 'HIGH' ELSE 'LOW' END, device))
