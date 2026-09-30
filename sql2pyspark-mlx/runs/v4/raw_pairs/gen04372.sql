WITH latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM web_events
    QUALIFY ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY event_time, id) = 1
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAG(duration_sec, 1) OVER w_hist AS prev_duration_sec,
        SUM(duration_sec) OVER (PARTITION BY event_type ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_duration_sec,
        RANK() OVER (PARTITION BY event_type, page ORDER BY duration_sec DESC) AS duration_sec_rank,
        COUNT(duration_sec) OVER (PARTITION BY event_type ORDER BY CAST(event_time AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) AS duration_sec_count_90d
    FROM latest
    WINDOW w_hist AS (PARTITION BY event_type, page ORDER BY event_time, id)
),
joined AS (
    -- Attach sessions attributes
    SELECT
        src.*,
        p.browser,
        p.device,
        p.country
    FROM metrics src
    LEFT JOIN sessions p
        ON src.session_id = p.id
),
metrics_web_events AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAST_VALUE(browser, TRUE) OVER (PARTITION BY page ORDER BY running_duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_browser,
        LAG(prev_duration_sec, 1) OVER (PARTITION BY device ORDER BY running_duration_sec, id) AS prev_prev_duration_sec,
        ROW_NUMBER() OVER (PARTITION BY device ORDER BY duration_sec_count_90d DESC, id) AS event_seq,
        AVG(duration_sec_count_90d) OVER (PARTITION BY session_id ORDER BY CAST(event_time AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) AS duration_sec_count_90d_avg_90d
    FROM joined
)
-- Grouped metrics
SELECT
    DATE_TRUNC('QUARTER', event_time) AS quarter_start,
    DATE_TRUNC('WEEK', event_time) AS week_start,
    APPROX_PERCENTILE(running_duration_sec, ARRAY(0.25, 0.5, 0.75)) AS running_duration_sec_quartiles,
    COUNT(*) FILTER (WHERE page = '/search') AS search_rows,
    AVG(duration_sec_count_90d) FILTER (WHERE browser = 'Chrome') AS chrome_avg_duration_sec_count_90d,
    (UNIX_TIMESTAMP(MAX(event_time)) - UNIX_TIMESTAMP(MIN(event_time))) / 60 AS duration_min,
    COUNT(*) FILTER (WHERE country = 'TH') AS th_rows,
    ROUND(AVG(duration_sec), 2) AS avg_duration_sec,
    DENSE_RANK() OVER (PARTITION BY DATE_TRUNC('QUARTER', event_time) ORDER BY COUNT(*) FILTER (WHERE country = 'TH') DESC) AS th_rows_dense_rank
FROM metrics_web_events
GROUP BY DATE_TRUNC('QUARTER', event_time), DATE_TRUNC('WEEK', event_time)
HAVING search_rows >= 1
ORDER BY quarter_start DESC
