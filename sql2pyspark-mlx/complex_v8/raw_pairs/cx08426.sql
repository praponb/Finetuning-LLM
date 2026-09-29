WITH cleaned_tickets AS (
    -- Scope the tickets rows
    SELECT *
    FROM tickets
    WHERE HOUR(closed_at) BETWEEN 9 AND 13
),
with_stats AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        (SELECT COUNT(CASE WHEN t.event_type NOT IN ('reopened', 'created') THEN 1 END) FROM ticket_events t WHERE t.ticket_id = s.id) AS event_event_type_count,
        (SELECT MIN(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.id) AS first_event_at,
        (SELECT COUNT(*) FROM ticket_events t WHERE t.ticket_id = s.id) AS event_count
    FROM cleaned_tickets s
),
labeled AS (
    -- Derive helper columns
    SELECT
        id,
        agent_id,
        opened_at,
        priority,
        csat,
        tags,
        event_event_type_count,
        event_count,
        DATE_FORMAT(opened_at, 'yyyy-MM') AS opened_at_label,
        LOWER(REGEXP_REPLACE(TRIM(channel), ' ', '_')) AS channel_slug
    FROM with_stats
    WHERE csat BETWEEN 1 AND 4
),
enriched_agent AS (
    -- Join support_agents
    SELECT
        s.id,
        s.agent_id,
        s.opened_at,
        s.csat,
        s.tags,
        s.event_count,
        s.opened_at_label,
        su.agent_name,
        su.team AS agent_team,
        su.tier AS agent_tier
    FROM labeled s
    LEFT JOIN support_agents su ON su.id = s.agent_id
),
latest AS (
    -- Keep the earliest row per agent_tier
    SELECT
        id,
        agent_id,
        opened_at,
        csat,
        tags,
        event_count,
        opened_at_label,
        agent_name,
        agent_team,
        agent_tier
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY agent_tier ORDER BY opened_at, id) AS recency_rank
        FROM enriched_agent
    ) ranked
    WHERE recency_rank = 1
),
joined_agent AS (
    -- Join support_agents
    SELECT
        s.id,
        s.agent_id,
        s.opened_at,
        s.csat,
        s.event_count,
        s.opened_at_label,
        s.agent_name,
        s.agent_team,
        s.agent_tier,
        su.team AS agent_team_2
    FROM latest s
    JOIN support_agents su ON su.id = s.agent_id
    WHERE su.team <> 'Tech'
)
-- Final projection
SELECT
    id,
    agent_id,
    csat,
    event_count,
    opened_at_label,
    agent_name,
    agent_team,
    agent_tier,
    agent_team_2,
    opened_at + INTERVAL 2 HOURS AS opened_at_plus_2h
FROM joined_agent
ORDER BY agent_tier, id
LIMIT 20
