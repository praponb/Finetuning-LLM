WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CAST(duration_sec AS DOUBLE) duration_sec_num,
        COALESCE(page, 'UNKNOWN') page_clean
    FROM web_events
    WHERE page IS DISTINCT FROM '/search'
),
with_dims AS (
    -- Attach sessions attributes
    SELECT
        e.id,
        e.session_id,
        e.event_type,
        e.page,
        e.event_time,
        e.duration_sec,
        e.page_clean,
        b.browser,
        b.device,
        b.country
    FROM base e
    LEFT JOIN sessions b
        ON e.session_id = b.id
),
with_prev AS (
    -- Previous event time per key
    SELECT *, LAG(event_time) OVER (PARTITION BY country ORDER BY event_time ASC, id ASC) AS prev_event_time
    FROM with_dims
),
flagged AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_event_time IS NULL OR event_time > prev_event_time + INTERVAL '7' DAY THEN 1 ELSE 0 END AS is_new_burst
    FROM with_prev
),
sessions_web_events AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY country ORDER BY event_time ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id_2
    FROM flagged
),
with_dims_web_events AS (
    -- Attach sessions attributes
    SELECT s.*, p.country AS session_country
    FROM sessions_web_events s
    LEFT JOIN sessions p
        ON s.session_id = p.id
    WHERE p.browser = 'Edge'
),
joined AS (
    -- Attach sessions attributes
    SELECT
        e.*,
        d.browser AS session_browser,
        d.country AS session_country_2,
        d.device AS session_device
    FROM with_dims_web_events e
    LEFT JOIN sessions d
        ON e.session_id = d.id
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(session_id_2) OVER (PARTITION BY TO_DATE(prev_event_time)) prev_event_time_to_date_sum_session_id_2,
        RANK() OVER (PARTITION BY session_country ORDER BY duration_sec DESC) duration_sec_rank,
        RANK() OVER w_recent is_new_burst_rank,
        FIRST_VALUE(session_device, TRUE) OVER (PARTITION BY device ORDER BY session_id_2 ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_session_device,
        LAG(session_id_2, 1) OVER (PARTITION BY session_country_2 ORDER BY event_time ASC, id ASC) prev_session_id_2
    FROM joined
    WINDOW w_recent AS (PARTITION BY session_browser ORDER BY is_new_burst DESC)
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        ROUND(is_new_burst_rank * 0.9, 2) adjusted_is_new_burst_rank,
        COALESCE(device, 'DIRECT') device_filled
    FROM metrics
    WHERE event_time > TIMESTAMP '2023-06-24 17:00:00'
      AND browser IS DISTINCT FROM 'Edge'
)
-- Grouped metrics
SELECT
    session_browser,
    CASE WHEN prev_session_id_2 >= 6 THEN 'HIGH' ELSE 'LOW' END AS prev_session_id_2_tier,
    ROUND(AVG(session_id_2), 2) AS avg_session_id_2,
    MAX(event_time) AS last_seen,
    COUNT(DISTINCT device) AS distinct_devices
FROM cleaned
GROUP BY session_browser, CASE WHEN prev_session_id_2 >= 6 THEN 'HIGH' ELSE 'LOW' END
HAVING COUNT(DISTINCT device) > 1
ORDER BY avg_session_id_2 ASC NULLS FIRST;
