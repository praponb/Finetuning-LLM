WITH valid AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        csat,
        tags,
        CONCAT(priority, '#', CAST(agent_id AS STRING)) AS priority_ref,
        COALESCE(priority, 'UNKNOWN') AS priority_or_unknown
    FROM tickets
    WHERE agent_id IS NOT NULL
),
with_activity AS (
    -- Rows that have matching ticket_events
    SELECT s.*
    FROM valid s
    WHERE EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.id
              AND t.actor IS NOT NULL
        )
),
with_stats AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        (SELECT SUM(t.csat) FROM tickets t WHERE t.agent_id = s.agent_id) AS total_ticket_csat
    FROM with_activity s
),
scored AS (
    -- Derive helper columns
    SELECT
        *,
        LENGTH(TRIM(priority_or_unknown)) AS priority_or_unknown_len,
        CONCAT(priority_ref, '#', CAST(csat AS STRING)) AS priority_ref_ref
    FROM with_stats
    WHERE priority NOT IN ('P4', 'P3')
),
enriched_agent AS (
    -- Attach agent attributes
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.opened_at,
        s.priority,
        s.channel,
        s.priority_or_unknown,
        s.total_ticket_csat,
        s.priority_or_unknown_len,
        s.priority_ref_ref,
        su.agent_name,
        su.team AS agent_team
    FROM scored s
    LEFT JOIN support_agents su ON su.id = s.agent_id
)
-- Final projection
SELECT
    id,
    requester_id,
    agent_id,
    opened_at,
    priority,
    priority_or_unknown,
    total_ticket_csat,
    priority_or_unknown_len,
    priority_ref_ref,
    agent_name,
    ROUND(total_ticket_csat / NULLIF(requester_id, 0), 3) AS total_ticket_csat_per_requester_id
FROM enriched_agent
ORDER BY priority NULLS LAST, priority_or_unknown_len
