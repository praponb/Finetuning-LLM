WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CASE WHEN device IN ('desktop', 'mobile') THEN 'Group A' ELSE 'Other' END AS device_group,
        COALESCE(browser, 'OTHER') AS browser_clean
    FROM sessions
    WHERE browser IS NOT NULL
      AND browser IS DISTINCT FROM 'Firefox'
),
with_last AS (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.browser,
        s.country,
        s.device_group,
        s.browser_clean,
        x.web_event_duration_sec,
        x.event_type
    FROM valid s
    LEFT JOIN LATERAL (
            SELECT c.duration_sec AS web_event_duration_sec, c.event_type
            FROM web_events c
            WHERE c.session_id = s.id
              AND c.event_time <= s.started_at
            ORDER BY c.event_time DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        country,
        web_event_duration_sec,
        LEAD(web_event_duration_sec) OVER w_recent AS next_web_event_duration_sec,
        LAG(web_event_duration_sec) OVER w_recent AS prev_web_event_duration_sec,
        ROW_NUMBER() OVER w_recent AS seq_no,
        SUM(web_event_duration_sec) OVER (PARTITION BY browser ORDER BY web_event_duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_web_event_duration_sec
    FROM with_last
    WINDOW w_recent AS (PARTITION BY event_type ORDER BY web_event_duration_sec, id)
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        web_event_duration_sec,
        next_web_event_duration_sec,
        prev_web_event_duration_sec,
        running_web_event_duration_sec,
        MAX(running_web_event_duration_sec) OVER w_running AS last7_max_running_web_event_duration_sec,
        AVG(running_web_event_duration_sec) OVER w_running AS last7_avg_running_web_event_duration_sec,
        SUM(running_web_event_duration_sec) OVER w_running AS last7_sum_running_web_event_duration_sec
    FROM enriched
    WINDOW w_running AS (PARTITION BY country ORDER BY prev_web_event_duration_sec, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
),
latest_match AS (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        latest.event_type,
        latest.web_event_event_time
    FROM scored s
    LEFT JOIN LATERAL (
            SELECT c.event_type, c.event_time AS web_event_event_time
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.duration_sec DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
with_latest AS (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.prev_web_event_duration_sec,
        s.running_web_event_duration_sec,
        s.last7_avg_running_web_event_duration_sec,
        s.event_type,
        s.web_event_event_time,
        top1.web_event_page
    FROM latest_match s
    JOIN LATERAL (
            SELECT c.page AS web_event_page
            FROM web_events c
            WHERE c.session_id = s.id
              AND c.event_time <= s.web_event_event_time
            ORDER BY c.event_time DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
)
-- Grouped metrics
SELECT
    DATE_TRUNC('WEEK', web_event_event_time) AS week_start,
    APPROX_PERCENTILE(last7_avg_running_web_event_duration_sec, 0.9) AS last7_avg_running_web_event_duration_sec_p90,
    COUNT(*) FILTER (WHERE web_event_page = '/product') AS product_rows,
    SUM(CASE WHEN web_event_page = '/cart' THEN running_web_event_duration_sec ELSE 0 END) AS cart_running_web_event_duration_sec,
    APPROX_PERCENTILE(last7_avg_running_web_event_duration_sec, 0.95) AS last7_avg_running_web_event_duration_sec_p95
FROM with_latest
GROUP BY DATE_TRUNC('WEEK', web_event_event_time)
ORDER BY cart_running_web_event_duration_sec DESC;
