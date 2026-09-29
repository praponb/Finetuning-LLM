WITH filtered_tickets AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        channel,
        csat,
        tags,
        YEAR(closed_at) AS closed_at_year
    FROM tickets
),
joined AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        su.team AS agent_team,
        su.tier AS agent_tier,
        su.agent_name
    FROM filtered_tickets s
    JOIN support_agents su ON su.id = s.agent_id
),
screened AS (
    -- Rows with no matching tickets
    SELECT s.*
    FROM joined s
    WHERE NOT EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.agent_id = s.agent_id
              AND t.csat > 1
        )
      AND SIZE(s.tags) >= 2
),
tagged AS (
    -- Label and score rows
    SELECT
        id,
        requester_id,
        agent_id,
        channel,
        csat,
        tags,
        closed_at_year,
        agent_team,
        agent_tier,
        agent_name,
        ABS(requester_id - 8) AS requester_id_distance,
        COALESCE(agent_tier, 'UNKNOWN') AS agent_tier_or_unknown,
        requester_id + csat AS requester_id_plus_csat
    FROM screened
),
event_stats AS (
    -- Per-ticket ticket_events metrics
    SELECT ticket_id, MIN(event_at) AS first_event_at
    FROM ticket_events
    GROUP BY ticket_id
),
profiled AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        t.first_event_at
    FROM tagged s
    LEFT JOIN event_stats t ON t.ticket_id = s.id
)
-- Final projection
SELECT
    id,
    agent_id,
    channel,
    csat,
    tags,
    closed_at_year,
    agent_team,
    agent_name,
    requester_id_distance,
    agent_tier_or_unknown,
    requester_id_plus_csat,
    first_event_at,
    SUBSTRING(TRIM(agent_tier), 1, 3) AS agent_tier_prefix
FROM profiled
WHERE LOWER(agent_name) LIKE '%s %'
ORDER BY channel NULLS LAST
