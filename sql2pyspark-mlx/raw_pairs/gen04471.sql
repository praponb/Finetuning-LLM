WITH with_dims AS (
    -- Attach sessions attributes
    SELECT a.*, b.country
    FROM web_events a
    LEFT JOIN sessions b
        ON a.session_id = b.id
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(duration_sec) OVER (PARTITION BY event_type) event_type_avg_duration_sec,
        SUM(duration_sec) OVER (PARTITION BY session_id ORDER BY duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_duration_sec,
        LEAD(duration_sec, 1) OVER (ORDER BY event_time, id) next_duration_sec,
        AVG(duration_sec) OVER (ORDER BY duration_sec, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) duration_sec_avg_2,
        AVG(duration_sec) OVER (PARTITION BY page) page_avg_duration_sec
    FROM with_dims
    WHERE page = '/home'
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN event_time - LAG(event_time) OVER (PARTITION BY country ORDER BY event_time, id) > INTERVAL '60' DAY THEN 1 ELSE 0 END AS is_new_burst
    FROM metrics
),
islands AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        event_time,
        country,
        event_type_avg_duration_sec,
        running_duration_sec,
        next_duration_sec,
        SUM(is_new_burst) OVER (PARTITION BY country ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM with_gaps
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(event_time) OVER (PARTITION BY country ORDER BY event_time, id) IS NULL
                 OR event_time > LAG(event_time) OVER (PARTITION BY country ORDER BY event_time, id) + INTERVAL '90' DAY THEN 1 ELSE 0 END AS is_new_burst
    FROM islands
),
sessions_web_events AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY country ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM gaps
)
-- Grouped metrics
SELECT
    DATE_TRUNC('MONTH', event_time) AS activity_month,
    COUNT(*) FILTER (WHERE country = 'US') AS us_rows,
    SIZE(COLLECT_SET(country)) AS num_countries,
    MAX(running_duration_sec) FILTER (WHERE country = 'JP') AS jp_max_running_duration_sec
FROM sessions_web_events
GROUP BY DATE_TRUNC('MONTH', event_time)
