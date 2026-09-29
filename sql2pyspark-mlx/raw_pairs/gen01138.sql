WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        session_id,
        page,
        event_time,
        duration_sec,
        COALESCE(duration_sec, 0) duration_sec_filled,
        COALESCE(page, 'DIRECT') resolved_page
    FROM web_events
    WHERE event_time > DATE '2023-08-08'
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        session_id,
        page,
        event_time,
        duration_sec,
        duration_sec_filled,
        resolved_page,
        AVG(duration_sec) OVER w_hist duration_sec_avg_6_prior,
        FIRST_VALUE(duration_sec) IGNORE NULLS OVER w_ordered first_known_duration_sec
    FROM cleaned
    WHERE duration_sec_filled >= 74
    WINDOW w_hist AS (PARTITION BY page ORDER BY duration_sec, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), w_ordered AS (PARTITION BY MONTH(event_time), session_id ORDER BY duration_sec_filled, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
latest AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        session_id,
        page,
        event_time,
        duration_sec_filled
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY session_id, page ORDER BY first_known_duration_sec DESC, id) AS latest_rank
        FROM with_windows
    ) t
    WHERE latest_rank = 1
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN event_time - LAG(event_time) OVER (PARTITION BY session_id ORDER BY event_time, id) > INTERVAL 7 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM latest
),
sessions_web_events AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY session_id ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id_2
    FROM with_gaps
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(event_time) OVER (PARTITION BY page ORDER BY event_time, id) IS NULL
                 OR event_time > LAG(event_time) OVER (PARTITION BY page ORDER BY event_time, id) + INTERVAL 14 DAYS THEN 1 ELSE 0 END AS session_start_flag_2
    FROM sessions_web_events
),
sessions_web_events_2 AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag_2) OVER (PARTITION BY page ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
joined AS (
    -- Attach sessions attributes
    SELECT
        s.id,
        s.session_id,
        s.event_time,
        s.duration_sec_filled,
        s.session_id_2,
        d.device,
        d.country
    FROM sessions_web_events_2 s
    INNER JOIN sessions d
        ON s.session_id = d.id
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('MONTH', event_time) AS activity_month,
    session_id,
    DATE_TRUNC('QUARTER', event_time) AS quarter_start,
    MAX(event_time) AS last_event_time,
    COUNT(DISTINCT country) AS distinct_countries,
    COUNT(*) FILTER (WHERE country = 'DE') AS de_rows
FROM joined
GROUP BY CUBE (DATE_TRUNC('MONTH', event_time), session_id, DATE_TRUNC('QUARTER', event_time));
