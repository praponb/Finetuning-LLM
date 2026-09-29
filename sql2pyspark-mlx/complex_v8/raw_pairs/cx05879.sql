WITH scoped AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        priority,
        channel,
        csat,
        tags,
        ARRAY_CONTAINS(tags, 'bug') AS has_bug,
        INITCAP(TRIM(channel)) AS channel_clean,
        CONCAT(channel, '#', CAST(id AS STRING)) AS channel_ref
    FROM tickets
    WHERE (priority IN ('P2', 'P3', 'P4') OR requester_id < 23)
),
with_details_agent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.priority,
        s.csat,
        s.tags,
        s.has_bug,
        s.channel_clean,
        s.channel_ref,
        su.team AS agent_team,
        su.tier AS agent_tier,
        su.hired_on AS agent_hired_on
    FROM scoped s
    JOIN support_agents su ON su.id = s.agent_id AND su.agent_name IS NOT NULL
),
screened AS (
    -- Rows that have matching tickets
    SELECT s.*
    FROM with_details_agent s
    WHERE EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.agent_id = s.agent_id
        )
),
joined_agent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.priority,
        s.csat,
        s.channel_clean,
        s.channel_ref,
        s.agent_team,
        s.agent_hired_on,
        su.team AS agent_team_2,
        su.agent_name,
        su.tier AS agent_tier_2
    FROM screened s
    JOIN support_agents su ON su.id = s.agent_id
),
exploded_detail_events AS (
    -- One row per event
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.csat,
        s.channel_clean,
        s.channel_ref,
        s.agent_hired_on,
        s.agent_team_2,
        t.id AS event_id,
        t.event_at,
        t.event_type,
        t.actor AS event_actor
    FROM joined_agent s
    LEFT JOIN ticket_events t ON t.ticket_id = s.id
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LAST_VALUE(requester_id) OVER (PARTITION BY agent_id, event_id ORDER BY agent_hired_on DESC, id, event_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_requester_id,
        ROW_NUMBER() OVER w_agent_id_event_id_by_agent_h_2 AS rn,
        NTILE(5) OVER (PARTITION BY agent_id, event_id ORDER BY agent_hired_on DESC, id, event_id) AS agent_hired_on_quintile,
        LEAD(agent_hired_on, 2) OVER (PARTITION BY agent_id, event_id ORDER BY agent_hired_on DESC, id, event_id) AS next2_agent_hired_on
    FROM exploded_detail_events
    WINDOW w_agent_id_event_id_by_agent_h_2 AS (PARTITION BY agent_id, event_id ORDER BY agent_hired_on DESC, id, event_id)
)
SELECT
    id,
    requester_id,
    agent_id,
    csat,
    channel_clean,
    agent_hired_on,
    agent_team_2,
    event_id,
    event_type,
    event_actor,
    rn,
    agent_hired_on_quintile,
    next2_agent_hired_on
FROM metrics
WHERE next2_agent_hired_on IS NOT NULL
ORDER BY event_id DESC NULLS FIRST
