WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CAST(event_time AS DATE) event_time_day,
        DATEDIFF(CURRENT_DATE(), event_time) days_since_event_time
    FROM web_events
    WHERE page <> '/home'
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY event_type, session_id ORDER BY event_time_day) AS rn
        FROM valid
    ) x
    WHERE rn = 1
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(duration_sec) OVER w_ordered last7_avg_duration_sec,
        MIN(duration_sec) OVER w_ordered last7_min_duration_sec,
        MAX(duration_sec) OVER w_ordered last7_max_duration_sec,
        SUM(days_since_event_time) OVER w_hist running_days_since_event_time
    FROM ranked
    WINDOW w_ordered AS (PARTITION BY page ORDER BY days_since_event_time, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), w_hist AS (PARTITION BY page ORDER BY event_time_day, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        SUBSTRING(page, 1, 4) page_prefix,
        CAST(last7_min_duration_sec AS INT) last7_min_duration_sec_int,
        COALESCE(page, 'DIRECT') resolved_page
    FROM with_windows
    WHERE duration_sec < 347
)
-- Multi-level aggregation
SELECT
    CASE WHEN last7_max_duration_sec >= 400 THEN 'HIGH' ELSE 'LOW' END AS last7_max_duration_sec_tier,
    COUNT(CASE WHEN page = '/checkout' THEN 1 END) AS checkout_count,
    ROUND(SUM(last7_min_duration_sec) / NULLIF(COUNT(CASE WHEN event_type = 'search' THEN 1 END), 0), 2) AS last7_min_duration_sec_per_search,
    SUM(last7_min_duration_sec) FILTER (WHERE event_type = 'click') AS click_total_last7_min_duration_sec
FROM cleaned
GROUP BY 1
ORDER BY last7_min_duration_sec_per_search DESC
