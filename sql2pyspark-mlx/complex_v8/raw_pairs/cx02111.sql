WITH scoped_tickets AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        tags,
        CASE WHEN priority IN ('P2', 'P1') THEN 'group_a' WHEN priority = 'P3' THEN 'group_b' ELSE 'other' END AS priority_group,
        GREATEST(csat, requester_id) AS greatest_csat_requester_id,
        DATE_TRUNC('QUARTER', closed_at) AS closed_at_quarter
    FROM tickets
    WHERE HOUR(opened_at) BETWEEN 8 AND 10
),
event_stats AS (
    -- Per-ticket ticket_events metrics
    SELECT
        ticket_id,
        COUNT(*) AS event_count,
        MAX(event_at) AS last_event_at,
        MIN(event_at) AS first_event_at
    FROM ticket_events
    GROUP BY ticket_id
),
profiled AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        COALESCE(t.event_count, 0) AS event_count,
        t.last_event_at,
        t.first_event_at
    FROM scoped_tickets s
    LEFT JOIN event_stats t ON t.ticket_id = s.id
),
without_matches AS (
    -- Keep rows whose agent matches
    SELECT s.*
    FROM profiled s
    WHERE EXISTS (
            SELECT 1
            FROM support_agents su
            WHERE su.id = s.agent_id
              AND YEAR(su.hired_on) = 2025
              AND su.team IN ('Onboarding', 'VIP')
        )
)
SELECT id, metric, value
FROM (
    SELECT id, event_count, greatest_csat_requester_id, requester_id
    FROM without_matches
) src
UNPIVOT (
    value FOR metric IN (event_count, greatest_csat_requester_id, requester_id)
) unp
