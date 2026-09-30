WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CASE WHEN event_type = 'click' THEN 1 ELSE 0 END AS is_click,
        COALESCE(page, 'DIRECT') AS page_filled
    FROM web_events
    WHERE duration_sec BETWEEN 231 AND 235
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN event_time - LAG(event_time) OVER (PARTITION BY session_id ORDER BY event_time ASC, id ASC) > INTERVAL 60 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM cleaned
),
islands AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        session_id,
        page,
        event_time,
        duration_sec,
        SUM(session_start_flag) OVER (PARTITION BY session_id ORDER BY event_time ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM with_gaps
),
joined AS (
    -- Attach sessions attributes
    SELECT e.*, d.country
    FROM islands e
    LEFT JOIN sessions d
        ON e.session_id = d.id
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        page,
        event_time,
        duration_sec,
        country
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY page ORDER BY duration_sec ASC, id ASC) AS dedup_rank
        FROM joined
    ) x
    WHERE dedup_rank = 1
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(duration_sec) OVER (PARTITION BY country, page ORDER BY CAST(event_time AS BIGINT) ASC RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) AS duration_sec_sum_30d,
        LAG(duration_sec, 2) OVER (PARTITION BY DATE_TRUNC('MONTH', event_time), country ORDER BY event_time ASC, id ASC) AS prev_duration_sec,
        NTILE(10) OVER (PARTITION BY DATE_TRUNC('month', event_time), page ORDER BY duration_sec DESC, id ASC) AS duration_sec_bucket,
        MAX(duration_sec) OVER (PARTITION BY TO_DATE(event_time), country ORDER BY CAST(event_time AS BIGINT) ASC RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS duration_sec_max_7d
    FROM latest
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(event_time) OVER (PARTITION BY country ORDER BY event_time ASC, id ASC) IS NULL
                 OR event_time > LAG(event_time) OVER (PARTITION BY country ORDER BY event_time ASC, id ASC) + INTERVAL '60' DAY THEN 1 ELSE 0 END AS is_new_session
    FROM with_windows
),
islands_web_events AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        page,
        event_time,
        duration_sec,
        country,
        duration_sec_sum_30d,
        prev_duration_sec,
        duration_sec_max_7d,
        SUM(is_new_session) OVER (PARTITION BY country ORDER BY event_time ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM flagged
)
SELECT
    id,
    page,
    duration_sec,
    country,
    duration_sec_sum_30d,
    prev_duration_sec,
    duration_sec_max_7d,
    island_id
FROM islands_web_events
ORDER BY prev_duration_sec DESC;
