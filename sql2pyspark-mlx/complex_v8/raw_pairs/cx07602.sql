WITH scoped_tickets AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        csat,
        closed_at + INTERVAL 30 MINUTES AS closed_at_plus_30m,
        DATEDIFF(DATE '2025-06-30', closed_at) AS days_since_closed_at,
        COALESCE(requester_id, 0) AS requester_id_filled
    FROM tickets
    WHERE channel = 'email'
),
without_matches AS (
    -- Rows with no matching ticket_events
    SELECT s.*
    FROM scoped_tickets s
    WHERE NOT EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.id
              AND MONTH(t.event_at) IN (2, 3)
              AND t.actor = 'agent'
        )
),
ticket_stats AS (
    -- Per-agent tickets metrics
    SELECT agent_id, ROUND(AVG(csat), 2) AS avg_ticket_csat
    FROM tickets
    GROUP BY agent_id
),
with_stats AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        t.avg_ticket_csat
    FROM without_matches s
    LEFT JOIN ticket_stats t ON t.agent_id = s.agent_id
)
SELECT
    id,
    requester_id,
    csat,
    closed_at_plus_30m,
    days_since_closed_at,
    requester_id_filled,
    avg_ticket_csat,
    ABS(days_since_closed_at - 315) AS days_since_closed_at_distance,
    DATE_FORMAT(closed_at, 'yyyy-MM') AS closed_at_label
FROM with_stats
