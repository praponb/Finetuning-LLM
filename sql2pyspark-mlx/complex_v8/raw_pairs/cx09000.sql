WITH recent AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
),
with_prev AS (
    -- Previous event_at per actor
    SELECT *, LAG(event_at) OVER (PARTITION BY actor ORDER BY event_at, id) AS prev_event_at
    FROM recent
),
flagged AS (
    -- Flag rows that start a new session (gap > 45 minutes)
    SELECT *, CASE WHEN prev_event_at IS NULL OR (UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(prev_event_at)) / 60 > 45 THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY actor ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
)
-- Subtotals with GROUPING SETS
SELECT
    ticket_id,
    COALESCE(actor, 'ALL') AS actor,
    session_id,
    MIN(prev_event_at) AS first_prev_event_at
FROM sessionized
GROUP BY GROUPING SETS ((ticket_id, actor, session_id), (ticket_id), (actor))
ORDER BY ticket_id NULLS LAST, actor NULLS LAST, session_id NULLS LAST
