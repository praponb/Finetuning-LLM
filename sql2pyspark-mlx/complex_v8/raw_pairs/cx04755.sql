WITH recent_events AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE event_at >= TIMESTAMP '2025-02-06 09:00:00'
),
latest_per_key AS (
    -- Keep the latest row per event_type, ticket_id
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY event_type, ticket_id ORDER BY event_at DESC, id DESC) AS rn
        FROM recent_events
    ) ranked
    WHERE rn = 1
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        ticket_id,
        event_type,
        NTILE(10) OVER (PARTITION BY actor, event_type ORDER BY event_at DESC, id) AS event_at_decile,
        ROUND(PERCENT_RANK() OVER w_ticket_id_by_event_at_ties, 3) AS event_at_percent_rank
    FROM latest_per_key
    WINDOW w_ticket_id_by_event_at_ties AS (PARTITION BY ticket_id ORDER BY event_at)
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.ticket_id,
        s.event_type,
        s.event_at_decile,
        s.event_at_percent_rank,
        t.tags AS ticket_tags,
        t.channel AS ticket_channel,
        su.team AS agent_team,
        su.tier AS agent_tier
    FROM metrics s
    LEFT JOIN tickets t ON t.id = s.ticket_id
    LEFT JOIN support_agents su ON su.id = t.agent_id
),
qualified AS (
    -- Rows that have matching ticket_events
    SELECT s.*
    FROM combined_details s
    WHERE EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.ticket_id
              AND t.actor IS NOT NULL
              AND t.event_type IS NOT NULL
        )
),
latest AS (
    -- Keep the latest row per event_type, ticket_id
    SELECT
        id,
        ticket_id,
        event_type,
        event_at_decile,
        event_at_percent_rank,
        ticket_tags,
        ticket_channel,
        agent_team,
        agent_tier
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY event_type, ticket_id ORDER BY event_at_percent_rank DESC, id DESC) AS row_num
        FROM qualified
    ) ranked
    WHERE row_num = 1
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_type,
    event_at_decile,
    event_at_percent_rank,
    ticket_tags,
    ticket_channel,
    agent_team,
    agent_tier
FROM latest
WHERE agent_tier NOT IN ('T3', 'T2')
ORDER BY event_type DESC, agent_tier DESC, id
LIMIT 5
