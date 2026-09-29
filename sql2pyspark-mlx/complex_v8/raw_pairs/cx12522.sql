WITH filtered AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATE_FORMAT(event_at, 'MMM yyyy') AS event_at_label
    FROM ticket_events
    WHERE actor <> 'customer'
),
with_prev AS (
    -- Previous event_at per actor
    SELECT *, LAG(event_at) OVER (PARTITION BY actor ORDER BY event_at, id) AS prev_event_at
    FROM filtered
),
flagged AS (
    -- Flag rows that start a new session (gap > 60 minutes)
    SELECT *, CASE WHEN prev_event_at IS NULL OR (UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(prev_event_at)) / 60 > 60 THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY actor ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
latest AS (
    -- Keep the latest row per event_type, actor
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        event_at_label,
        prev_event_at,
        starts_session,
        session_id
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY event_type, actor ORDER BY session_id DESC, id DESC) AS recency_rank
        FROM sessionized
    ) ranked
    WHERE recency_rank = 1
),
ranked AS (
    -- Previous / next values per key
    SELECT
        id,
        ticket_id,
        event_at,
        event_at_label,
        prev_event_at,
        starts_session,
        session_id,
        SUM(starts_session) OVER (PARTITION BY event_type ORDER BY session_id, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_starts_session,
        ROUND(session_id - AVG(session_id) OVER (PARTITION BY event_type), 2) AS session_id_vs_event_type_avg,
        NTILE(10) OVER (PARTITION BY event_type ORDER BY session_id, id) AS session_id_decile
    FROM latest
)
SELECT id, measure, metric_value
FROM (
    SELECT id, running_starts_session, session_id_decile, starts_session, session_id
    FROM ranked
) src
UNPIVOT (
    metric_value FOR measure IN (running_starts_session, session_id_decile, starts_session, session_id)
) unp
ORDER BY id, measure
