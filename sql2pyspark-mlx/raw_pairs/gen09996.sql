WITH prev_events AS (
    SELECT *, LAG(event_time) OVER (PARTITION BY session_id ORDER BY event_time ASC, id ASC) AS prev_event_time
    FROM web_events
),
gaps AS (
    SELECT *, CASE WHEN prev_event_time IS NULL OR event_time > prev_event_time + INTERVAL 90 DAYS THEN 1 ELSE 0 END AS is_new_burst
    FROM prev_events
),
sessions_web_events AS (
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY session_id ORDER BY event_time ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM gaps
),
joined AS (
    SELECT
        src.*,
        b.browser,
        b.country
    FROM sessions_web_events src
    LEFT JOIN sessions b
        ON src.session_id = b.id
),
enriched AS (
    SELECT
        id,
        event_type,
        page,
        country,
        AVG(duration_sec) OVER w_part AS running_avg_duration_sec,
        MIN(duration_sec) OVER w_part AS running_min_duration_sec,
        SUM(duration_sec) OVER w_part AS running_sum_duration_sec
    FROM joined
    WHERE page IS NOT NULL
    WINDOW w_part AS (PARTITION BY session_id, event_type ORDER BY is_new_burst ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
metrics AS (
    SELECT
        *,
        LAG(running_min_duration_sec) OVER (PARTITION BY page ORDER BY running_avg_duration_sec ASC, id ASC) AS prev_running_min_duration_sec,
        LEAD(running_avg_duration_sec) OVER (PARTITION BY page ORDER BY running_avg_duration_sec ASC, id ASC) AS next_running_avg_duration_sec,
        LAG(running_avg_duration_sec) OVER (PARTITION BY page ORDER BY running_avg_duration_sec ASC, id ASC) AS prev_running_avg_duration_sec,
        ROW_NUMBER() OVER (PARTITION BY page ORDER BY running_avg_duration_sec ASC, id ASC) AS seq_no
    FROM enriched
),
filtered AS (
    SELECT
        *,
        CASE WHEN page = '/cart' THEN 1 ELSE 0 END AS is_cart,
        next_running_avg_duration_sec / NULLIF(running_sum_duration_sec, 0) AS next_running_avg_duration_sec_per_running_sum_duration_sec
    FROM metrics
    WHERE next_running_avg_duration_sec <= 330
),
metrics_web_events AS (
    SELECT
        id,
        event_type,
        running_min_duration_sec,
        prev_running_min_duration_sec,
        next_running_avg_duration_sec,
        prev_running_avg_duration_sec,
        seq_no,
        is_cart,
        MIN(prev_running_avg_duration_sec) OVER w_ordered AS running_min_prev_running_avg_duration_sec,
        COUNT(prev_running_avg_duration_sec) OVER w_ordered AS running_count_prev_running_avg_duration_sec,
        MAX(prev_running_avg_duration_sec) OVER w_ordered AS running_max_prev_running_avg_duration_sec,
        LAG(running_min_duration_sec, 1) OVER w_ordered_2 AS prev_running_min_duration_sec_2
    FROM filtered
    WINDOW w_ordered AS (PARTITION BY country ORDER BY next_running_avg_duration_sec ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_ordered_2 AS (PARTITION BY event_type, country ORDER BY prev_running_avg_duration_sec ASC, id ASC)
)
SELECT
    CASE WHEN next_running_avg_duration_sec >= 440 THEN 'PREMIUM' ELSE 'STANDARD' END AS next_running_avg_duration_sec_tier,
    event_type,
    CASE WHEN is_cart >= 68 THEN 'PREMIUM' ELSE 'STANDARD' END AS is_cart_tier,
    COUNT_IF(event_type = 'page_view') AS page_view_events,
    COUNT(CASE WHEN event_type = 'search' THEN 1 END) AS search_count
FROM metrics_web_events
GROUP BY CASE WHEN next_running_avg_duration_sec >= 440 THEN 'PREMIUM' ELSE 'STANDARD' END, event_type, CASE WHEN is_cart >= 68 THEN 'PREMIUM' ELSE 'STANDARD' END
ORDER BY event_type ASC
