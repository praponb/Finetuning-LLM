WITH valid_tickets AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        csat,
        tags
    FROM tickets
    WHERE channel <> 'chat'
      AND requester_id < 17
),
agent_id_stats AS (
    -- Avg csat per agent_id
    SELECT agent_id, AVG(csat) AS avg_csat
    FROM valid_tickets
    GROUP BY agent_id
),
compared AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.avg_csat, ROUND(s.csat - g.avg_csat, 2) AS csat_minus_avg
    FROM valid_tickets s
    JOIN agent_id_stats g ON g.agent_id = s.agent_id
    WHERE s.csat > g.avg_csat
),
without_matches AS (
    -- Compare against the overall min
    SELECT *
    FROM compared
    WHERE requester_id > (SELECT MIN(requester_id) FROM compared)
),
with_children AS (
    -- One row per event
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.opened_at,
        s.closed_at,
        s.priority,
        s.channel,
        s.tags,
        s.avg_csat,
        t.id AS event_id,
        t.actor AS event_actor,
        t.event_type,
        t.event_at
    FROM without_matches s
    JOIN ticket_events t ON t.ticket_id = s.id
)
-- Final projection
SELECT
    id,
    requester_id,
    agent_id,
    priority,
    channel,
    tags,
    avg_csat,
    event_id,
    event_actor,
    event_type
FROM with_children
ORDER BY requester_id DESC
