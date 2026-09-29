WITH scoped AS (
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
        tags
    FROM tickets
    WHERE SIZE(tags) >= 1
      AND csat >= 3
),
combined_details AS (
    -- Attach agent attributes
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.opened_at,
        s.closed_at,
        s.priority,
        s.channel,
        s.csat,
        s.tags,
        su.tier AS agent_tier
    FROM scoped s
    JOIN support_agents su ON su.id = s.agent_id AND su.tier NOT IN ('T1', 'T2')
    WHERE su.tier IS NOT NULL
),
screened AS (
    -- Compare against the overall avg
    SELECT *
    FROM combined_details
    WHERE csat > (SELECT AVG(csat) FROM combined_details)
),
line_level AS (
    -- One row per event
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.opened_at,
        s.priority,
        s.channel,
        s.tags,
        t.id AS event_id,
        t.event_at,
        t.event_type,
        t.actor AS event_actor
    FROM screened s
    JOIN ticket_events t ON t.ticket_id = s.id
),
per_label AS (
    -- One row per tag
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.opened_at,
        s.priority,
        s.channel,
        s.event_id,
        s.event_at,
        s.event_type,
        s.event_actor,
        t.tag
    FROM line_level s
    LATERAL VIEW OUTER EXPLODE(s.tags) t AS tag
),
filtered AS (
    -- Rows with no matching ticket_events
    SELECT s.*
    FROM per_label s
    WHERE NOT EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.id
              AND t.actor = 'system'
              AND t.event_type = 'created'
        )
),
ticket_stats AS (
    -- Per-agent tickets metrics
    SELECT agent_id, MIN(opened_at) AS first_ticket_opened_at
    FROM tickets
    GROUP BY agent_id
),
summarized_rows AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        t.first_ticket_opened_at
    FROM filtered s
    LEFT JOIN ticket_stats t ON t.agent_id = s.agent_id
)
SELECT
    id,
    requester_id,
    agent_id,
    opened_at,
    priority,
    channel,
    event_id,
    event_at,
    event_type,
    tag,
    FLOOR(requester_id / 5) * 5 AS requester_id_bucket
FROM summarized_rows
ORDER BY requester_id_bucket, event_id DESC
