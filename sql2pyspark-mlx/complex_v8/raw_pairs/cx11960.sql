WITH base AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        COALESCE(actor, 'UNKNOWN') AS actor_or_unknown,
        CASE WHEN event_type IN ('assigned', 'reopened') THEN 'group_a' WHEN event_type = 'escalated' THEN 'group_b' ELSE 'other' END AS event_type_group
    FROM ticket_events
    WHERE ticket_id <= 36
),
with_parent_ticket AS (
    -- Attach ticket attributes
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        s.actor_or_unknown,
        s.event_type_group,
        t.tags AS ticket_tags,
        t.closed_at AS ticket_closed_at,
        t.requester_id AS ticket_requester_id
    FROM base s
    JOIN tickets t ON t.id = s.ticket_id
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        COUNT(*) OVER w_actor_or_unknown AS rows_in_actor_or_unknown,
        DENSE_RANK() OVER (ORDER BY event_at ASC, id) AS event_at_dense_rank
    FROM with_parent_ticket
    WINDOW w_actor_or_unknown AS (PARTITION BY actor_or_unknown)
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_type,
    actor,
    actor_or_unknown,
    ticket_tags,
    ticket_requester_id,
    rows_in_actor_or_unknown,
    event_at_dense_rank,
    ABS(ticket_requester_id - 17) AS ticket_requester_id_distance
FROM with_windows
ORDER BY event_at_dense_rank DESC NULLS FIRST, rows_in_actor_or_unknown DESC
