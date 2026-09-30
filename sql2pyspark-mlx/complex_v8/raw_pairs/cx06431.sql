WITH cleaned_events AS (
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
    -- Previous event_at per event_type
    SELECT *, LAG(event_at) OVER (PARTITION BY event_type ORDER BY event_at, id) AS prev_event_at
    FROM cleaned_events
),
flagged AS (
    -- Flag rows that start a new session (gap > 2 hours)
    SELECT *, CASE WHEN prev_event_at IS NULL OR event_at > prev_event_at + INTERVAL 2 HOURS THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY event_type ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
tagged AS (
    -- Add calculated columns
    SELECT
        *,
        CONCAT(actor, '#', CAST(id AS STRING)) AS actor_ref,
        QUARTER(event_at) AS event_at_quarter,
        ROUND(session_id / NULLIF(is_new_session, 0), 3) AS session_id_per_is_new_session
    FROM sessionized
    WHERE prev_event_at >= TIMESTAMP '2025-01-31 09:00:00'
),
with_details_ticket AS (
    -- Join tickets
    SELECT
        s.*,
        t.requester_id AS ticket_requester_id,
        t.opened_at AS ticket_opened_at,
        su.tier AS agent_tier
    FROM tagged s
    JOIN tickets t ON t.id = s.ticket_id
    JOIN support_agents su ON su.id = t.agent_id
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_at,
    event_type,
    actor,
    prev_event_at,
    is_new_session,
    actor_ref,
    event_at_quarter,
    session_id_per_is_new_session,
    ticket_requester_id,
    ticket_opened_at
FROM with_details_ticket
WHERE TO_DATE(prev_event_at) < DATE '2025-02-06'
ORDER BY event_at
