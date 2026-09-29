WITH recent AS (
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
        tags,
        ARRAY_CONTAINS(tags, 'refund') AS has_refund,
        DATE_FORMAT(opened_at, 'yyyy-MM') AS opened_at_label,
        ABS(requester_id - 13) AS requester_id_distance
    FROM tickets
),
event_stats AS (
    -- Per-ticket ticket_events metrics
    SELECT
        ticket_id,
        MIN(event_at) AS first_event_at,
        COUNT(*) FILTER (WHERE event_type IN ('replied', 'escalated', 'created')) AS event_event_type_count
    FROM ticket_events
    GROUP BY ticket_id
),
summarized_rows AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        t.first_event_at,
        COALESCE(t.event_event_type_count, 0) AS event_event_type_count
    FROM recent s
    LEFT JOIN event_stats t ON t.ticket_id = s.id
),
filtered AS (
    -- Rows with no matching tickets
    SELECT s.*
    FROM summarized_rows s
    WHERE NOT EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.agent_id = s.agent_id
              AND t.closed_at >= TIMESTAMP '2025-01-17 21:00:00'
        )
      AND s.channel = 'web'
),
combined_details AS (
    -- Attach agent attributes
    SELECT s.*, su.tier AS agent_tier, su.team AS agent_team
    FROM filtered s
    LEFT JOIN support_agents su ON su.id = s.agent_id
),
line_level AS (
    -- One row per event
    SELECT
        s.id,
        s.agent_id,
        s.opened_at,
        s.closed_at,
        s.priority,
        s.opened_at_label,
        s.requester_id_distance,
        s.first_event_at,
        s.event_event_type_count,
        s.agent_tier,
        s.agent_team,
        t.id AS event_id,
        t.event_type,
        t.actor AS event_actor,
        t.event_at
    FROM combined_details s
    JOIN ticket_events t ON t.ticket_id = s.id
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, event_id, metric, metric_value
FROM (
    SELECT id, event_id, event_event_type_count, requester_id_distance
    FROM line_level
) src
UNPIVOT (
    metric_value FOR metric IN (event_event_type_count, requester_id_distance)
) unp
