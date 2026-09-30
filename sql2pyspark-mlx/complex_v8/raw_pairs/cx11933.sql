WITH base AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        priority,
        csat,
        FLOOR(requester_id / 5) * 5 AS requester_id_bucket,
        DATE_FORMAT(opened_at, 'yyyy-MM') AS opened_at_label,
        DATEDIFF(DATE '2025-06-30', opened_at) AS days_since_opened_at
    FROM tickets
    WHERE opened_at < TIMESTAMP '2025-02-24 15:00:00'
),
agent_id_stats AS (
    -- Avg days_since_opened_at per agent_id
    SELECT agent_id, AVG(days_since_opened_at) AS avg_days_since_opened_at
    FROM base
    GROUP BY agent_id
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_days_since_opened_at,
        ROUND(s.days_since_opened_at - g.avg_days_since_opened_at, 2) AS days_since_opened_at_minus_avg
    FROM base s
    JOIN agent_id_stats g ON g.agent_id = s.agent_id
    WHERE s.days_since_opened_at > g.avg_days_since_opened_at
),
detail_rows_events AS (
    -- One row per event
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.priority,
        s.csat,
        s.requester_id_bucket,
        s.days_since_opened_at,
        s.avg_days_since_opened_at,
        s.days_since_opened_at_minus_avg,
        t.id AS event_id,
        t.actor AS event_actor,
        t.event_type
    FROM outliers s
    JOIN ticket_events t ON t.ticket_id = s.id AND t.actor = 'agent'
)
-- Subtotals with GROUPING SETS
SELECT
    csat,
    event_id,
    COALESCE(event_type, 'ALL') AS event_type,
    COUNT(*) AS n_rows
FROM detail_rows_events
GROUP BY GROUPING SETS ((csat, event_id, event_type), (csat), (event_id))
ORDER BY csat NULLS LAST, event_id NULLS LAST, event_type NULLS LAST
