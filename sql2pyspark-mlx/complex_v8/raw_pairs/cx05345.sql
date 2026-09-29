WITH base_tickets AS (
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
        DATE_FORMAT(opened_at, 'yyyy-MM-dd') AS opened_at_label,
        HOUR(closed_at) AS closed_at_hour
    FROM tickets
    WHERE opened_at >= TIMESTAMP '2025-01-13 09:00:00'
),
priority_stats AS (
    -- Max csat per priority
    SELECT priority, MAX(csat) AS max_csat
    FROM base_tickets
    GROUP BY priority
),
compared AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.max_csat, ROUND(s.csat - g.max_csat, 2) AS csat_minus_max
    FROM base_tickets s
    JOIN priority_stats g ON g.priority = s.priority
    WHERE s.csat = g.max_csat
),
joined_agent AS (
    -- Enrich with dimension data
    SELECT s.*, su.agent_name, su.team AS agent_team
    FROM compared s
    JOIN support_agents su ON su.id = s.agent_id
),
tagged AS (
    -- Label and score rows
    SELECT *, DATE_FORMAT(opened_at, 'yyyy-MM-dd') AS opened_at_label_2
    FROM joined_agent
    WHERE priority <> 'P3'
)
-- Final projection
SELECT
    id,
    requester_id,
    agent_id,
    opened_at,
    closed_at,
    priority,
    channel,
    opened_at_label,
    closed_at_hour,
    max_csat,
    agent_name,
    LOWER(REGEXP_REPLACE(TRIM(agent_name), ' ', '_')) AS agent_name_slug
FROM tagged
