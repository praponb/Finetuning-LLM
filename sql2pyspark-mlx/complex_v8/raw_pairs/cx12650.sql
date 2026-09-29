WITH valid AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATE_TRUNC('HOUR', event_at) AS event_at_hour,
        DATE_FORMAT(event_at, 'MMM yyyy') AS event_at_label,
        DATEDIFF(DATE '2025-06-30', event_at) AS days_since_event_at
    FROM ticket_events
    WHERE ticket_id IS NOT NULL
      AND event_type IN ('replied', 'resolved', 'reopened')
),
qualified AS (
    -- Compare against the overall max
    SELECT *
    FROM valid
    WHERE days_since_event_at < (SELECT MAX(days_since_event_at) FROM valid)
),
with_details_ticket AS (
    -- Join tickets
    SELECT
        s.id,
        s.ticket_id,
        s.event_type,
        s.actor,
        s.event_at_hour,
        s.event_at_label,
        s.days_since_event_at,
        t.agent_id AS ticket_agent_id
    FROM qualified s
    LEFT JOIN tickets t ON t.id = s.ticket_id
),
actor_stats AS (
    -- Max days_since_event_at per actor
    SELECT actor, MAX(days_since_event_at) AS max_days_since_event_at
    FROM with_details_ticket
    GROUP BY actor
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_days_since_event_at,
        ROUND(s.days_since_event_at - g.max_days_since_event_at, 2) AS days_since_event_at_minus_max
    FROM with_details_ticket s
    JOIN actor_stats g ON g.actor = s.actor
    WHERE s.days_since_event_at = g.max_days_since_event_at
)
SELECT
    id,
    ticket_id,
    event_type,
    actor,
    days_since_event_at,
    ticket_agent_id,
    days_since_event_at_minus_max
FROM outliers
ORDER BY ticket_id DESC
