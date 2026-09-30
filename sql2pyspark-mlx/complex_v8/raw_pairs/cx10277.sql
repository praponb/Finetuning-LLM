WITH cleaned_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATEDIFF(DATE '2026-01-01', event_at) AS days_since_event_at,
        LOWER(REGEXP_REPLACE(TRIM(event_type), ' ', '_')) AS event_type_slug,
        event_at + INTERVAL 15 MINUTES AS event_at_plus_15m
    FROM ticket_events
    WHERE actor = 'agent'
),
qualified AS (
    -- Rows that have matching ticket_events
    SELECT s.*
    FROM cleaned_events s
    WHERE EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.ticket_id
        )
),
with_activity AS (
    -- Rows that have matching ticket_events
    SELECT s.*
    FROM qualified s
    WHERE EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.ticket_id
        )
      AND s.actor NOT IN ('agent', 'customer')
),
event_type_stats AS (
    -- Median days_since_event_at per event_type
    SELECT event_type, PERCENTILE_APPROX(days_since_event_at, 0.5) AS median_days_since_event_at
    FROM with_activity
    GROUP BY event_type
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_days_since_event_at,
        ROUND(s.days_since_event_at - g.median_days_since_event_at, 2) AS days_since_event_at_minus_median
    FROM with_activity s
    JOIN event_type_stats g ON g.event_type = s.event_type
    WHERE s.days_since_event_at >= g.median_days_since_event_at
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        NTILE(3) OVER w_ticket_id_event_type_by_days AS days_since_event_at_tercile,
        RANK() OVER w_ticket_id_event_type_by_days AS days_since_event_at_rank,
        ROUND(AVG(median_days_since_event_at) OVER (PARTITION BY ticket_id, event_type ORDER BY days_since_event_at, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS median_days_since_event_at_avg_4
    FROM outliers
    WINDOW w_ticket_id_event_type_by_days AS (PARTITION BY ticket_id, event_type ORDER BY days_since_event_at, id)
)
SELECT
    id,
    ticket_id,
    event_at,
    event_type,
    actor,
    days_since_event_at,
    event_type_slug,
    event_at_plus_15m,
    median_days_since_event_at,
    days_since_event_at_rank,
    median_days_since_event_at_avg_4
FROM with_trends
ORDER BY days_since_event_at DESC, actor DESC
