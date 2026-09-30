WITH valid_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATEDIFF(DATE '2026-01-01', event_at) AS days_since_event_at,
        event_at + INTERVAL 1 DAYS AS event_at_plus_1d,
        DATE_TRUNC('YEAR', event_at) AS event_at_year
    FROM ticket_events
    WHERE event_at >= TIMESTAMP '2025-01-13 14:00:00'
      AND event_type = 'reopened'
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.ticket_id,
        s.event_type,
        s.actor,
        s.event_at_plus_1d,
        s.event_at_year,
        t.tags AS ticket_tags,
        t.requester_id AS ticket_requester_id,
        t.priority AS ticket_priority
    FROM valid_events s
    JOIN tickets t ON t.id = s.ticket_id
    WHERE t.requester_id <= 23
),
event_type_stats AS (
    -- Median ticket_requester_id per event_type
    SELECT event_type, PERCENTILE_APPROX(ticket_requester_id, 0.5) AS median_ticket_requester_id
    FROM combined_details
    GROUP BY event_type
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_ticket_requester_id,
        ROUND(s.ticket_requester_id - g.median_ticket_requester_id, 2) AS ticket_requester_id_minus_median
    FROM combined_details s
    JOIN event_type_stats g ON g.event_type = s.event_type
    WHERE s.ticket_requester_id >= g.median_ticket_requester_id
),
combined_details_2 AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        t.csat AS ticket_csat,
        t.requester_id AS ticket_requester_id_2,
        t.channel AS ticket_channel
    FROM above_average s
    JOIN tickets t ON t.id = s.ticket_id
)
-- Final projection
SELECT
    id,
    event_type,
    actor,
    event_at_plus_1d,
    ticket_requester_id,
    ticket_priority,
    median_ticket_requester_id,
    ticket_csat,
    ticket_requester_id_2,
    ticket_channel
FROM combined_details_2
ORDER BY ticket_priority DESC NULLS FIRST, event_at_plus_1d, id
LIMIT 20
