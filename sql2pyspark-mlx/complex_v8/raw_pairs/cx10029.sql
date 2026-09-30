WITH scoped_tickets AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        csat,
        COALESCE(csat, 0) AS csat_filled
    FROM tickets
    WHERE SIZE(tags) >= 1
),
qualified AS (
    -- Rows with no matching ticket_events
    SELECT s.*
    FROM scoped_tickets s
    WHERE NOT EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.id
              AND t.event_type <> 'reopened'
              AND t.event_at >= TIMESTAMP '2025-01-31 10:00:00'
        )
),
ticket_stats AS (
    -- Per-agent tickets metrics
    SELECT agent_id, COUNT(*) AS ticket_count
    FROM tickets
    GROUP BY agent_id
),
summarized_rows AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        COALESCE(t.ticket_count, 0) AS ticket_count
    FROM qualified s
    LEFT JOIN ticket_stats t ON t.agent_id = s.agent_id
),
ticket_stats_2 AS (
    -- Per-agent tickets metrics
    SELECT agent_id, COUNT(*) AS ticket_count_2, COUNT(*) AS ticket_count_3
    FROM tickets
    GROUP BY agent_id
),
with_stats AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        COALESCE(t.ticket_count_2, 0) AS ticket_count_2,
        COALESCE(t.ticket_count_3, 0) AS ticket_count_3
    FROM summarized_rows s
    LEFT JOIN ticket_stats_2 t ON t.agent_id = s.agent_id
),
with_activity AS (
    -- Rows with no matching ticket_events
    SELECT s.*
    FROM with_stats s
    WHERE NOT EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.id
              AND t.event_type = 'reopened'
              AND t.actor IN ('customer', 'system')
        )
)
-- Final projection
SELECT
    id,
    requester_id,
    agent_id,
    opened_at,
    priority,
    channel,
    csat,
    ticket_count_3,
    SUBSTRING(TRIM(priority), 1, 2) AS priority_prefix
FROM with_activity
WHERE agent_id >= 9
ORDER BY agent_id
