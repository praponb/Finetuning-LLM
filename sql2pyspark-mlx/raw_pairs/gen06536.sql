WITH cleaned AS (
    SELECT
        id,
        device,
        browser,
        DAYOFWEEK(started_at) AS started_at_dayofweek
    FROM sessions
    WHERE customer_id IS NOT NULL
      AND HOUR(started_at) BETWEEN 10 AND 13
),
cleaned_sessions AS (
    SELECT
        *,
        FLOOR(started_at_dayofweek) AS started_at_dayofweek_whole,
        TRIM(browser) AS clean_browser,
        COALESCE(browser, 'DIRECT') AS resolved_browser
    FROM cleaned
    WHERE device != 'desktop'
),
with_windows AS (
    SELECT
        id,
        device,
        browser,
        started_at_dayofweek_whole,
        SUM(started_at_dayofweek_whole) OVER (PARTITION BY device, browser ORDER BY started_at_dayofweek_whole, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_started_at_dayofweek_whole,
        MAX(started_at_dayofweek_whole) OVER (PARTITION BY device, browser ORDER BY started_at_dayofweek_whole, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_started_at_dayofweek_whole,
        MIN(started_at_dayofweek_whole) OVER (PARTITION BY device, browser ORDER BY started_at_dayofweek_whole, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_started_at_dayofweek_whole,
        COUNT(started_at_dayofweek_whole) OVER (PARTITION BY device, browser ORDER BY started_at_dayofweek_whole, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_started_at_dayofweek_whole
    FROM cleaned_sessions
),
latest_match AS (
    SELECT
        s.*,
        top1.event_time,
        top1.event_type
    FROM with_windows s
    LEFT JOIN LATERAL (
            SELECT c.event_time, c.event_type
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.duration_sec DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
latest_match_sessions AS (
    SELECT s.*, top1.web_event_event_time
    FROM latest_match s
    LEFT JOIN LATERAL (
            SELECT c.event_time AS web_event_event_time
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.event_time DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
)
SELECT
    CASE WHEN running_min_started_at_dayofweek_whole >= 61 THEN 'PREMIUM' ELSE 'STANDARD' END AS running_min_started_at_dayofweek_whole_tier,
    DATE_TRUNC('QUARTER', event_time) AS quarter_start,
    ROUND(SUM(running_min_started_at_dayofweek_whole) / NULLIF(COUNT(CASE WHEN device = 'desktop' THEN 1 END), 0), 2) AS running_min_started_at_dayofweek_whole_per_desktop,
    COUNT(DISTINCT id) AS distinct_ids
FROM latest_match_sessions
GROUP BY CASE WHEN running_min_started_at_dayofweek_whole >= 61 THEN 'PREMIUM' ELSE 'STANDARD' END, DATE_TRUNC('QUARTER', event_time)
ORDER BY running_min_started_at_dayofweek_whole_per_desktop DESC;
