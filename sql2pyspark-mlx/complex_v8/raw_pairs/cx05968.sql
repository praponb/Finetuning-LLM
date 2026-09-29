WITH filtered_tickets AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        channel,
        csat,
        LOWER(REGEXP_REPLACE(TRIM(channel), ' ', '_')) AS channel_slug,
        CONCAT(channel, '#', CAST(agent_id AS STRING)) AS channel_ref
    FROM tickets
    WHERE channel IN ('email', 'web')
      AND opened_at >= TIMESTAMP '2025-01-25 11:00:00'
),
without_matches AS (
    -- Compare against the overall max
    SELECT *
    FROM filtered_tickets
    WHERE csat < (SELECT MAX(csat) FROM filtered_tickets)
),
with_children AS (
    -- One row per event
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.opened_at,
        s.closed_at,
        s.channel,
        s.csat,
        s.channel_slug,
        s.channel_ref,
        t.id AS event_id,
        t.event_type,
        t.event_at
    FROM without_matches s
    LEFT JOIN ticket_events t ON t.ticket_id = s.id
),
with_parent AS (
    -- Attach agent attributes
    SELECT
        s.id,
        s.agent_id,
        s.opened_at,
        s.closed_at,
        s.channel_slug,
        s.channel_ref,
        s.event_id,
        s.event_type,
        s.event_at,
        su.agent_name,
        su.team AS agent_team,
        su.hired_on AS agent_hired_on,
        t.actor AS event_actor
    FROM with_children s
    JOIN support_agents su ON su.id = s.agent_id
    JOIN ticket_events t ON t.id = s.event_id
),
annotated AS (
    -- Label and score rows
    SELECT *, TRUNC(agent_hired_on, 'MM') AS agent_hired_on_month_start
    FROM with_parent
    WHERE agent_id IS NOT NULL
)
SELECT
    id,
    agent_id,
    opened_at,
    channel_slug,
    channel_ref,
    event_id,
    event_type,
    event_at,
    agent_team,
    agent_hired_on,
    event_actor,
    agent_hired_on_month_start,
    TRUNC(agent_hired_on, 'MM') AS agent_hired_on_month_start_2,
    COALESCE(agent_team, 'UNKNOWN') AS agent_team_or_unknown
FROM annotated
ORDER BY agent_id DESC NULLS FIRST, agent_team_or_unknown
