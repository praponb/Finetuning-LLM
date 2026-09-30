WITH recent_events AS (
    -- Scope the ticket_events rows
    SELECT *
    FROM ticket_events
    WHERE ticket_id IS NOT NULL
      AND event_at >= TIMESTAMP '2025-02-06 10:00:00'
),
enriched AS (
    -- Attach ticket attributes
    SELECT
        s.*,
        t.priority AS ticket_priority,
        t.tags AS ticket_tags,
        t.requester_id AS ticket_requester_id,
        su.hired_on AS agent_hired_on,
        su.tier AS agent_tier
    FROM recent_events s
    JOIN tickets t ON t.id = s.ticket_id AND t.requester_id < 21
    JOIN support_agents su ON su.id = t.agent_id
),
without_matches AS (
    -- Keep rows whose ticket matches
    SELECT s.*
    FROM enriched s
    WHERE EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.id = s.ticket_id
              AND t.opened_at < TIMESTAMP '2025-02-24 09:00:00'
        )
)
SELECT
    COALESCE(agent_tier, 'ALL') AS agent_tier,
    COALESCE(actor, 'ALL') AS actor,
    COALESCE(event_type, 'ALL') AS event_type,
    MAX(event_at) AS last_event_at,
    COUNT(*) AS n_rows,
    MIN(agent_hired_on) AS first_agent_hired_on
FROM without_matches
GROUP BY GROUPING SETS ((agent_tier, actor, event_type), (agent_tier), ())
ORDER BY agent_tier NULLS LAST, actor NULLS LAST, event_type NULLS LAST
