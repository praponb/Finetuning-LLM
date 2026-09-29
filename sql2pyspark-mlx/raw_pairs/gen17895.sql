WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, INITCAP(TRIM(browser)) AS browser_title
    FROM sessions
    WHERE country != 'SG'
),
with_last AS (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.device,
        s.started_at,
        latest.web_event_event_type
    FROM valid s
    JOIN LATERAL (
            SELECT c.event_type AS web_event_event_type
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.event_time DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN started_at - LAG(started_at) OVER (PARTITION BY web_event_event_type ORDER BY started_at, id) > INTERVAL '30' DAY THEN 1 ELSE 0 END AS is_new_session
    FROM with_last
),
islands AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        device,
        started_at,
        web_event_event_type,
        SUM(is_new_session) OVER (PARTITION BY web_event_event_type ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM gaps
),
valid_sessions AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        device,
        started_at,
        web_event_event_type,
        burst_id,
        DAY(started_at) AS started_at_day,
        COALESCE(web_event_event_type, 'N/A') AS web_event_event_type_clean
    FROM islands
    WHERE web_event_event_type IS NOT NULL
      AND TO_DATE(started_at) >= '2024-06-10'
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        device,
        web_event_event_type,
        burst_id,
        started_at_day,
        LAG(burst_id, 1, 0) OVER w_ordered AS prev_burst_id,
        ROW_NUMBER() OVER (PARTITION BY web_event_event_type ORDER BY started_at_day, id) AS seq_no
    FROM valid_sessions
    WINDOW w_ordered AS (PARTITION BY web_event_event_type ORDER BY burst_id, id)
)
SELECT
    id,
    web_event_event_type,
    burst_id,
    started_at_day,
    prev_burst_id,
    seq_no
FROM metrics
