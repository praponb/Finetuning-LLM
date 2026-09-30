WITH base AS (
    -- Filter tickets
    SELECT
        id,
        agent_id,
        opened_at,
        channel,
        csat,
        tags,
        CASE WHEN channel IN ('chat', 'phone') THEN 'group_a' WHEN channel = 'web' THEN 'group_b' ELSE 'other' END AS channel_group
    FROM tickets
    WHERE closed_at < TIMESTAMP '2025-02-21 07:00:00'
),
line_level AS (
    -- One row per event
    SELECT
        s.id,
        s.agent_id,
        s.channel,
        s.csat,
        s.tags,
        t.id AS event_id,
        t.event_type,
        t.event_at,
        t.actor AS event_actor
    FROM base s
    JOIN ticket_events t ON t.ticket_id = s.id
),
scored AS (
    -- Add calculated columns
    SELECT
        id,
        agent_id,
        csat,
        event_id,
        event_type,
        event_at,
        DATEDIFF(DATE '2025-12-31', event_at) AS days_since_event_at,
        CASE WHEN csat >= 4 THEN 'large' WHEN csat >= 3 THEN 'medium' ELSE 'small' END AS csat_tier
    FROM line_level
    WHERE event_type IN ('reopened', 'created', 'replied')
)
-- Subtotals with CUBE
SELECT
    agent_id,
    event_id,
    csat,
    MAX(event_at) AS last_event_at,
    COUNT(*) FILTER (WHERE event_type <> 'reopened') AS n_event_type_reopened,
    ROUND(SUM(days_since_event_at) / NULLIF(SUM(csat), 0), 3) AS days_since_event_at_to_csat_ratio
FROM scored
GROUP BY CUBE(agent_id, event_id, csat)
