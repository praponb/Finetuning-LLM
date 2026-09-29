WITH valid AS (
    -- Filter tickets
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        tags,
        DATE_TRUNC('YEAR', opened_at) AS opened_at_year,
        DAYOFWEEK(closed_at) AS closed_at_dayofweek,
        LEAST(requester_id, csat) AS least_requester_id_csat
    FROM tickets
    WHERE requester_id BETWEEN 4 AND 25
),
with_parent AS (
    -- Join support_agents
    SELECT
        s.*,
        su.team AS agent_team,
        su.hired_on AS agent_hired_on,
        su.tier AS agent_tier
    FROM valid s
    JOIN support_agents su ON su.id = s.agent_id AND su.team IN ('Billing', 'Tech')
),
event_stats AS (
    -- Per-ticket ticket_events metrics
    SELECT ticket_id, MAX(event_at) AS last_event_at
    FROM ticket_events
    GROUP BY ticket_id
),
with_counts AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        t.last_event_at
    FROM with_parent s
    LEFT JOIN event_stats t ON t.ticket_id = s.id
)
SELECT
    id,
    agent_id,
    opened_at,
    closed_at,
    priority,
    tags,
    opened_at_year,
    least_requester_id_csat,
    agent_team,
    agent_hired_on,
    agent_tier,
    last_event_at
FROM with_counts
WHERE priority = 'P2'
ORDER BY agent_hired_on DESC NULLS FIRST
