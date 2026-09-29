WITH cleaned_events AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        COALESCE(actor, 'UNKNOWN') AS actor_or_unknown
    FROM ticket_events
    WHERE event_type = 'reopened'
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        id,
        ticket_id,
        event_at,
        actor,
        actor_or_unknown,
        ROUND(PERCENT_RANK() OVER (ORDER BY event_at, id), 3) AS event_at_percent_rank
    FROM cleaned_events
),
with_details AS (
    -- Join tickets
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.actor_or_unknown,
        t.closed_at AS ticket_closed_at,
        t.requester_id AS ticket_requester_id,
        t.tags AS ticket_tags
    FROM sequenced s
    LEFT JOIN tickets t ON t.id = s.ticket_id
)
SELECT
    ticket_id,
    actor_or_unknown,
    GROUPING_ID() AS gid,
    MAX(ticket_closed_at) AS last_ticket_closed_at
FROM with_details
GROUP BY GROUPING SETS ((ticket_id, actor_or_unknown), (ticket_id), (actor_or_unknown))
ORDER BY ticket_id NULLS LAST, actor_or_unknown NULLS LAST
