WITH base_events AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        LOWER(REGEXP_REPLACE(TRIM(actor), ' ', '_')) AS actor_slug
    FROM ticket_events
),
with_prev AS (
    -- Previous event_at per ticket_id
    SELECT *, LAG(event_at) OVER (PARTITION BY ticket_id ORDER BY event_at, id) AS prev_event_at
    FROM base_events
),
flagged AS (
    -- Flag rows that start a new session (gap > 2 hours)
    SELECT *, CASE WHEN prev_event_at IS NULL OR event_at > prev_event_at + INTERVAL 2 HOURS THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY ticket_id ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
derived AS (
    -- Label and score rows
    SELECT
        *,
        LOWER(TRIM(event_type)) AS event_type_clean,
        CASE WHEN session_id >= 4 THEN 'A' WHEN session_id >= 2 THEN 'B' ELSE 'C' END AS session_id_tier,
        LENGTH(TRIM(actor)) AS actor_len
    FROM sessionized
    WHERE session_id >= 3
),
scored AS (
    -- Add calculated columns
    SELECT
        id,
        ticket_id,
        event_at,
        actor,
        actor_slug,
        starts_session,
        session_id,
        event_type_clean,
        session_id_tier,
        actor_len,
        ROUND((UNIX_TIMESTAMP(prev_event_at) - UNIX_TIMESTAMP(event_at)) / 3600, 1) AS hours_event_at_to_prev_event_at,
        COALESCE(session_id_tier, 'UNKNOWN') AS session_id_tier_or_unknown,
        COALESCE(event_type, 'UNKNOWN') AS event_type_or_unknown
    FROM derived
)
SELECT
    id,
    event_at,
    actor,
    starts_session,
    session_id,
    actor_len,
    hours_event_at_to_prev_event_at,
    event_type_or_unknown
FROM scored
ORDER BY actor NULLS LAST
