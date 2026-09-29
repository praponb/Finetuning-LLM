WITH recent_tickets AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        priority,
        channel,
        csat,
        tags,
        UPPER(TRIM(channel)) AS channel_clean,
        SIZE(tags) AS tags_count,
        csat + requester_id AS csat_plus_requester_id
    FROM tickets
    WHERE agent_id >= 3
      AND priority IS NOT NULL
),
with_parent_agent AS (
    -- Join support_agents
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.channel,
        s.csat,
        s.tags,
        s.channel_clean,
        s.tags_count,
        s.csat_plus_requester_id,
        su.agent_name
    FROM recent_tickets s
    JOIN support_agents su ON su.id = s.agent_id AND UPPER(TRIM(su.agent_name)) LIKE 'J%'
),
without_matches AS (
    -- Rows with no matching ticket_events
    SELECT s.*
    FROM with_parent_agent s
    WHERE NOT EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.id
              AND t.event_type = 'created'
        )
),
with_details_agent AS (
    -- Attach agent attributes
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.channel,
        s.csat,
        s.tags,
        s.channel_clean,
        s.tags_count,
        s.csat_plus_requester_id,
        s.agent_name,
        su.team AS agent_team,
        su.hired_on AS agent_hired_on,
        su.tier AS agent_tier
    FROM without_matches s
    JOIN support_agents su ON su.id = s.agent_id
    WHERE su.tier IS NOT NULL
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT agent_tier, channel, csat_plus_requester_id
    FROM with_details_agent
) src
PIVOT (
    MAX(csat_plus_requester_id)
    FOR channel IN ('email', 'chat', 'phone', 'web')
)
