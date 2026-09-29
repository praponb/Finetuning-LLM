WITH valid_tickets AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        tags,
        LOWER(TRIM(priority)) AS priority_clean,
        DATE_TRUNC('QUARTER', closed_at) AS closed_at_quarter,
        ROUND(csat / NULLIF(requester_id, 0), 3) AS csat_per_requester_id
    FROM tickets
),
ticket_stats AS (
    -- Per-agent tickets metrics
    SELECT
        agent_id,
        SUM(csat) AS total_ticket_csat,
        COUNT(*) AS ticket_count,
        MAX(csat) AS max_ticket_csat
    FROM tickets
    GROUP BY agent_id
),
with_counts AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        t.total_ticket_csat,
        COALESCE(t.ticket_count, 0) AS ticket_count,
        t.max_ticket_csat
    FROM valid_tickets s
    LEFT JOIN ticket_stats t ON t.agent_id = s.agent_id
),
combined_details_agent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.opened_at,
        s.closed_at,
        s.priority,
        s.tags,
        s.priority_clean,
        s.closed_at_quarter,
        s.csat_per_requester_id,
        s.total_ticket_csat,
        s.ticket_count,
        su.agent_name
    FROM with_counts s
    JOIN support_agents su ON su.id = s.agent_id
    WHERE UPPER(TRIM(su.agent_name)) LIKE 'K%'
),
combined_details AS (
    -- Join support_agents
    SELECT s.*, su.agent_name AS agent_agent_name, su.team AS agent_team
    FROM combined_details_agent s
    JOIN support_agents su ON su.id = s.agent_id
)
-- Final projection
SELECT
    id,
    requester_id,
    opened_at,
    closed_at,
    priority,
    priority_clean,
    closed_at_quarter,
    ticket_count,
    agent_name,
    agent_agent_name,
    agent_team,
    SIZE(tags) AS tags_count,
    CONCAT_WS(': ', agent_agent_name, priority_clean) AS agent_agent_name_priority_clean_label
FROM combined_details
ORDER BY opened_at, agent_agent_name, id
LIMIT 20
