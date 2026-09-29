WITH prepared AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATE_TRUNC('DAY', event_at) AS event_at_day,
        DATE_FORMAT(event_at, 'MMM yyyy') AS event_at_label,
        CONCAT(event_type, '#', CAST(id AS STRING)) AS event_type_ref
    FROM ticket_events
    WHERE ticket_id IS NOT NULL
      AND event_type <> 'assigned'
),
summarized_rows AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        (SELECT MAX(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS last_event_at,
        (SELECT COUNT(CASE WHEN t.actor = 'agent' THEN 1 END) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS event_actor_count,
        (SELECT MAX(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS last_event_at_2
    FROM prepared s
),
latest AS (
    -- Keep the earliest row per actor, ticket_id
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        event_at_day,
        event_at_label,
        event_type_ref,
        last_event_at,
        event_actor_count,
        last_event_at_2
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY actor, ticket_id ORDER BY event_at_day, id) AS rn
        FROM summarized_rows
    ) ranked
    WHERE rn = 1
),
with_parent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.ticket_id,
        s.event_type,
        s.actor,
        s.event_at_day,
        s.event_at_label,
        s.event_type_ref,
        s.last_event_at,
        s.last_event_at_2,
        t.channel AS ticket_channel,
        su.team AS agent_team,
        su.agent_name
    FROM latest s
    JOIN tickets t ON t.id = s.ticket_id
    JOIN support_agents su ON su.id = t.agent_id
),
with_parent_2 AS (
    -- Attach ticket attributes
    SELECT
        s.id,
        s.ticket_id,
        s.event_type,
        s.actor,
        s.event_at_day,
        s.event_at_label,
        s.event_type_ref,
        s.last_event_at,
        s.last_event_at_2,
        s.ticket_channel,
        s.agent_name,
        t.channel AS ticket_channel_2,
        t.csat AS ticket_csat,
        t.priority AS ticket_priority,
        su.team AS agent_team_2
    FROM with_parent s
    LEFT JOIN tickets t ON t.id = s.ticket_id
    LEFT JOIN support_agents su ON su.id = t.agent_id
),
with_parent_3 AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.ticket_id,
        s.event_type,
        s.actor,
        s.event_at_day,
        s.event_at_label,
        s.event_type_ref,
        s.last_event_at,
        s.last_event_at_2,
        s.ticket_channel,
        s.agent_name,
        s.ticket_channel_2,
        s.ticket_csat,
        s.agent_team_2,
        t.requester_id AS ticket_requester_id,
        t.channel AS ticket_channel_3,
        t.opened_at AS ticket_opened_at,
        su.team AS agent_team
    FROM with_parent_2 s
    JOIN tickets t ON t.id = s.ticket_id
    JOIN support_agents su ON su.id = t.agent_id
    WHERE t.channel IN ('phone', 'web', 'chat')
),
event_stats_2 AS (
    -- Per-ticket ticket_events metrics
    SELECT
        ticket_id,
        COUNT(*) AS event_count,
        COUNT(*) FILTER (WHERE event_type = 'resolved') AS event_event_type_count
    FROM ticket_events
    GROUP BY ticket_id
),
summarized_rows_2 AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        COALESCE(t.event_count, 0) AS event_count,
        COALESCE(t.event_event_type_count, 0) AS event_event_type_count
    FROM with_parent_3 s
    LEFT JOIN event_stats_2 t ON t.ticket_id = s.ticket_id
)
-- Final projection
SELECT
    id,
    ticket_id,
    actor,
    event_at_day,
    last_event_at,
    ticket_channel,
    agent_name,
    ticket_channel_2,
    ticket_csat,
    agent_team_2,
    ticket_requester_id,
    ticket_opened_at,
    agent_team,
    event_count,
    event_event_type_count,
    ABS(ticket_requester_id - 18) AS ticket_requester_id_distance
FROM summarized_rows_2
WHERE HOUR(last_event_at) BETWEEN 7 AND 12
ORDER BY ticket_requester_id_distance DESC, event_count
