WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CASE WHEN event_type IN ('reopened', 'replied') THEN 'group_a' WHEN event_type = 'assigned' THEN 'group_b' ELSE 'other' END AS event_type_group,
        CONCAT_WS(': ', actor, event_type) AS actor_event_type_label,
        CONCAT_WS(': ', actor, event_type) AS actor_event_type_label_2
    FROM ticket_events
    WHERE ticket_id IS NOT NULL
      AND HOUR(event_at) BETWEEN 8 AND 12
),
without_matches AS (
    -- Keep rows whose ticket matches
    SELECT s.*
    FROM valid s
    WHERE EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.id = s.ticket_id
              AND t.closed_at < TIMESTAMP '2025-03-08 23:00:00'
        )
),
qualified AS (
    -- Rows that have matching ticket_events
    SELECT s.*
    FROM without_matches s
    WHERE EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.ticket_id
        )
),
with_parent_ticket AS (
    -- Join tickets
    SELECT
        s.id,
        s.ticket_id,
        s.actor,
        s.event_type_group,
        s.actor_event_type_label,
        s.actor_event_type_label_2,
        t.priority AS ticket_priority,
        t.closed_at AS ticket_closed_at,
        t.agent_id AS ticket_agent_id,
        su.team AS agent_team,
        su.tier AS agent_tier
    FROM qualified s
    JOIN tickets t ON t.id = s.ticket_id
    LEFT JOIN support_agents su ON su.id = t.agent_id
    WHERE t.agent_id <= 5
)
-- Final projection
SELECT
    id,
    ticket_id,
    actor,
    actor_event_type_label,
    actor_event_type_label_2,
    ticket_priority,
    ticket_closed_at,
    ticket_agent_id,
    agent_team,
    agent_tier,
    CONCAT(actor, '#', CAST(id AS STRING)) AS actor_ref,
    LENGTH(TRIM(actor)) AS actor_len
FROM with_parent_ticket
ORDER BY actor_event_type_label DESC NULLS FIRST
