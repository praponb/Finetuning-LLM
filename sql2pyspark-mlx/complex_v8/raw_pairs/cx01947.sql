WITH prepared AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        csat,
        tags,
        requester_id + csat AS requester_id_plus_csat,
        csat + requester_id AS csat_plus_requester_id,
        DATE_FORMAT(closed_at, 'MMM yyyy') AS closed_at_label
    FROM tickets
    WHERE channel IN ('web', 'chat', 'email')
      AND closed_at >= TIMESTAMP '2025-02-04 23:00:00'
),
event_stats AS (
    -- Per-ticket ticket_events metrics
    SELECT
        ticket_id,
        COUNT(*) AS event_count,
        COUNT(*) FILTER (WHERE actor NOT IN ('agent', 'system')) AS event_actor_count
    FROM ticket_events
    GROUP BY ticket_id
),
profiled AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        COALESCE(t.event_count, 0) AS event_count,
        COALESCE(t.event_actor_count, 0) AS event_actor_count
    FROM prepared s
    LEFT JOIN event_stats t ON t.ticket_id = s.id
),
latest AS (
    -- Keep the latest row per agent_id
    SELECT
        id,
        requester_id,
        agent_id,
        csat,
        tags,
        requester_id_plus_csat,
        csat_plus_requester_id,
        closed_at_label,
        event_count,
        event_actor_count
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY agent_id ORDER BY event_count DESC, id DESC) AS rn
        FROM profiled
    ) ranked
    WHERE rn = 1
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, metric, metric_value
FROM (
    SELECT id, requester_id, requester_id_plus_csat, event_actor_count
    FROM latest
) src
UNPIVOT INCLUDE NULLS (
    metric_value FOR metric IN (requester_id AS m_requester_id, requester_id_plus_csat AS requester_id_plus_csat_v, event_actor_count AS event_actor_count_v)
) unp
ORDER BY id, metric
