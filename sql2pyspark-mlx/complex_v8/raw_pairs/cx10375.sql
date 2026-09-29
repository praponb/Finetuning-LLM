WITH valid_tickets AS (
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
        tags,
        DATE_FORMAT(closed_at, 'yyyy-MM') AS closed_at_label,
        LEAST(csat, requester_id) AS least_csat_requester_id,
        DATE_TRUNC('HOUR', closed_at) AS closed_at_hour
    FROM tickets
    WHERE MONTH(opened_at) IN (2, 3)
      AND channel IN ('email', 'chat')
),
enriched AS (
    -- Add calculated columns
    SELECT
        id,
        requester_id,
        opened_at,
        closed_at,
        priority,
        channel,
        tags,
        closed_at_label,
        least_csat_requester_id,
        closed_at_hour,
        CONCAT_WS(' / ', channel, closed_at_label) AS channel_closed_at_label_label
    FROM valid_tickets
),
event_stats AS (
    -- Per-ticket ticket_events metrics
    SELECT
        ticket_id,
        COUNT(*) AS event_count,
        COUNT(CASE WHEN event_type = 'created' THEN 1 END) AS event_event_type_count,
        COUNT(*) AS event_count_2
    FROM ticket_events
    GROUP BY ticket_id
),
profiled AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        COALESCE(t.event_count, 0) AS event_count,
        COALESCE(t.event_event_type_count, 0) AS event_event_type_count,
        COALESCE(t.event_count_2, 0) AS event_count_2
    FROM enriched s
    LEFT JOIN event_stats t ON t.ticket_id = s.id
),
filtered AS (
    -- Rows that have matching ticket_events
    SELECT s.*
    FROM profiled s
    WHERE s.id IN (
            SELECT t.ticket_id
            FROM ticket_events t
            WHERE t.event_type IN ('resolved', 'reopened', 'assigned')
        )
      AND s.event_count_2 <= 7
)
-- Final projection
SELECT
    id,
    requester_id,
    opened_at,
    closed_at,
    priority,
    channel,
    tags,
    closed_at_label,
    least_csat_requester_id,
    closed_at_hour,
    channel_closed_at_label_label,
    event_count,
    event_event_type_count,
    event_count_2
FROM filtered
ORDER BY least_csat_requester_id
