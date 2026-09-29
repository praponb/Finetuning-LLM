WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        LENGTH(TRIM(event_type)) AS event_type_len
    FROM ticket_events
),
with_prev AS (
    -- Previous event_at per ticket_id
    SELECT *, LAG(event_at) OVER (PARTITION BY ticket_id ORDER BY event_at, id) AS prev_event_at
    FROM base
),
flagged AS (
    -- Flag rows that start a new session (gap > 60 minutes)
    SELECT *, CASE WHEN prev_event_at IS NULL OR (UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(prev_event_at)) / 60 > 60 THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY ticket_id ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
metrics AS (
    -- Previous / next values per key
    SELECT
        id,
        event_at,
        event_type,
        actor,
        event_type_len,
        prev_event_at,
        session_id,
        LAST_VALUE(starts_session) OVER (ORDER BY prev_event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_starts_session,
        ROUND(AVG(event_type_len) OVER (ORDER BY prev_event_at DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS event_type_len_avg_1_prior
    FROM sessionized
),
latest_per_key AS (
    -- Keep the latest row per event_type
    SELECT
        id,
        event_at,
        event_type,
        actor,
        event_type_len,
        prev_event_at,
        session_id,
        last_starts_session,
        event_type_len_avg_1_prior
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY event_type_len_avg_1_prior DESC, id DESC) AS rn
        FROM metrics
    ) ranked
    WHERE rn = 1
)
-- Final projection
SELECT
    id,
    event_type,
    event_type_len,
    session_id,
    last_starts_session,
    event_type_len_avg_1_prior,
    DATE_FORMAT(event_at, 'yyyy-MM-dd HH:00') AS event_at_label,
    DATE_FORMAT(event_at, 'MMM yyyy') AS event_at_label_2
FROM latest_per_key
ORDER BY last_starts_session DESC, event_at_label, id
LIMIT 10
