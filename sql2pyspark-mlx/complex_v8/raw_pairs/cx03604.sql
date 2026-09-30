WITH recent AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE ticket_id <= 18
),
labeled AS (
    -- Label and score rows
    SELECT
        *,
        CONCAT_WS(' / ', actor, event_type) AS actor_event_type_label,
        SUBSTRING(TRIM(event_type), 1, 4) AS event_type_prefix
    FROM recent
    WHERE ticket_id <= 21
),
enriched AS (
    -- Attach ticket attributes
    SELECT s.*, t.agent_id AS ticket_agent_id, t.priority AS ticket_priority
    FROM labeled s
    JOIN tickets t ON t.id = s.ticket_id AND t.requester_id BETWEEN 7 AND 18
),
scored AS (
    -- Label and score rows
    SELECT *, MONTH(event_at) AS event_at_month
    FROM enriched
)
SELECT
    id,
    ticket_id,
    event_at,
    event_type,
    actor,
    actor_event_type_label,
    event_type_prefix,
    event_at_month,
    CASE WHEN ticket_priority IN ('P3', 'P4') THEN 'group_a' WHEN ticket_priority = 'P1' THEN 'group_b' ELSE 'other' END AS ticket_priority_group,
    ABS(event_at_month - 6) AS event_at_month_distance
FROM scored
ORDER BY actor_event_type_label DESC NULLS FIRST, id
LIMIT 5
