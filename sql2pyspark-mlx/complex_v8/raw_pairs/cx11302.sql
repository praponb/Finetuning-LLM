WITH prepared AS (
    -- Filter tickets
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        csat,
        tags,
        opened_at + INTERVAL 30 MINUTES AS opened_at_plus_30m
    FROM tickets
    WHERE agent_id IS NOT NULL
),
with_activity AS (
    -- Rows that have matching ticket_events
    SELECT s.*
    FROM prepared s
    WHERE s.id IN (
            SELECT t.ticket_id
            FROM ticket_events t
        )
),
with_parent_agent AS (
    -- Enrich with dimension data
    SELECT s.*, su.team AS agent_team
    FROM with_activity s
    JOIN support_agents su ON su.id = s.agent_id
),
with_details_agent AS (
    -- Attach agent attributes
    SELECT
        s.id,
        s.agent_id,
        s.closed_at,
        s.priority,
        s.channel,
        s.csat,
        s.tags,
        s.agent_team,
        su.tier AS agent_tier
    FROM with_parent_agent s
    JOIN support_agents su ON su.id = s.agent_id AND UPPER(TRIM(su.agent_name)) LIKE 'J%'
    WHERE su.tier IN ('T1', 'T3')
),
with_parent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.agent_id,
        s.closed_at,
        s.priority,
        s.csat,
        s.tags,
        s.agent_team,
        s.agent_tier,
        su.tier AS agent_tier_2,
        su.team AS agent_team_2
    FROM with_details_agent s
    JOIN support_agents su ON su.id = s.agent_id AND su.team IS NOT NULL
),
with_stats AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        (SELECT MAX(t.opened_at) FROM tickets t WHERE t.agent_id = s.agent_id) AS last_ticket_opened_at,
        (SELECT MIN(t.opened_at) FROM tickets t WHERE t.agent_id = s.agent_id) AS first_ticket_opened_at,
        (SELECT COUNT(*) FROM tickets t WHERE t.agent_id = s.agent_id) AS ticket_count
    FROM with_parent s
),
event_stats AS (
    -- Per-ticket ticket_events metrics
    SELECT ticket_id, COUNT(*) AS event_count
    FROM ticket_events
    GROUP BY ticket_id
),
with_counts AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        COALESCE(t.event_count, 0) AS event_count
    FROM with_stats s
    LEFT JOIN event_stats t ON t.ticket_id = s.id
)
SELECT
    id,
    closed_at,
    priority,
    csat,
    tags,
    agent_team,
    agent_tier_2,
    agent_team_2,
    first_ticket_opened_at,
    event_count
FROM with_counts
