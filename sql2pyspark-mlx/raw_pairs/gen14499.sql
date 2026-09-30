WITH scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        session_id,
        page,
        ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY duration_sec DESC, id) AS event_seq,
        LAST_VALUE(duration_sec) IGNORE NULLS OVER (PARTITION BY event_type ORDER BY duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_duration_sec,
        AVG(duration_sec) OVER (PARTITION BY event_type ORDER BY CAST(event_time AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) AS duration_sec_avg_30d
    FROM web_events
),
with_dims AS (
    -- Attach sessions attributes
    SELECT
        src.*,
        b.country,
        b.device
    FROM scored src
    LEFT JOIN sessions b
        ON src.session_id = b.id
    WHERE b.country = 'JP'
),
joined AS (
    -- Attach sessions attributes
    SELECT
        src.id,
        src.session_id,
        src.page,
        src.event_seq,
        src.last_known_duration_sec,
        src.country,
        src.device,
        s.device AS session_device
    FROM with_dims src
    LEFT JOIN sessions s
        ON src.session_id = s.id
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(last_known_duration_sec) OVER (PARTITION BY session_id ORDER BY event_seq, id) AS prev_last_known_duration_sec,
        LEAD(last_known_duration_sec) OVER (PARTITION BY session_id ORDER BY event_seq, id) AS next_last_known_duration_sec,
        LEAD(event_seq) OVER (PARTITION BY session_id ORDER BY event_seq, id) AS next_event_seq,
        LAG(event_seq) OVER (PARTITION BY session_id ORDER BY event_seq, id) AS prev_event_seq,
        ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY event_seq, id) AS row_seq
    FROM joined
)
-- Join a dimension and aggregate
SELECT
    p.country AS session_country,
    p.device AS session_device,
    s.device AS web_event_device,
    COUNT(*) AS num_rows,
    ROUND(AVG(s.prev_last_known_duration_sec), 2) AS avg_prev_last_known_duration_sec
FROM with_windows s
INNER JOIN sessions p ON s.session_id = p.id
GROUP BY GROUPING SETS ((p.country, p.device, s.device), (p.country, p.device), (p.country), ())
