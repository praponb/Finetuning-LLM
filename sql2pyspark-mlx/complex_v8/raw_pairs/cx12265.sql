WITH recent_tickets AS (
    -- Filter tickets
    SELECT *
    FROM tickets
    WHERE priority NOT IN ('P1', 'P2')
),
event_stats AS (
    -- Per-ticket ticket_events metrics
    SELECT
        ticket_id,
        MAX(event_at) AS last_event_at,
        COUNT(*) FILTER (WHERE event_type NOT IN ('escalated', 'resolved')) AS event_event_type_count,
        MAX(event_at) AS last_event_at_2
    FROM ticket_events
    GROUP BY ticket_id
),
with_stats AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        t.last_event_at,
        COALESCE(t.event_event_type_count, 0) AS event_event_type_count,
        t.last_event_at_2
    FROM recent_tickets s
    LEFT JOIN event_stats t ON t.ticket_id = s.id
),
latest_per_key AS (
    -- Keep the earliest row per channel
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
        last_event_at,
        event_event_type_count,
        last_event_at_2
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY channel ORDER BY last_event_at_2, id) AS rn
        FROM with_stats
    ) ranked
    WHERE rn = 1
),
tagged AS (
    -- Label and score rows
    SELECT
        *,
        DATEDIFF(DATE '2025-06-30', closed_at) AS days_since_closed_at,
        DATE_TRUNC('MONTH', closed_at) AS closed_at_month
    FROM latest_per_key
)
SELECT id, metric, amount
FROM (
    SELECT id, days_since_closed_at, csat, requester_id, event_event_type_count
    FROM tagged
) src
UNPIVOT (
    amount FOR metric IN (days_since_closed_at AS days_since_closed_at_v, csat AS m_csat, requester_id AS requester_id_v, event_event_type_count AS event_event_type_count_v)
) unp
ORDER BY id, metric
