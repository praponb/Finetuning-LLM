WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE (MONTH(event_at) IN (1, 3) OR event_type <> 'reopened')
),
enriched AS (
    -- Derive helper columns
    SELECT
        *,
        CONCAT_WS('-', actor, event_type) AS actor_event_type_label,
        DATEDIFF(DATE '2025-06-30', event_at) AS days_since_event_at,
        CONCAT_WS('-', event_type, actor) AS event_type_actor_label
    FROM cleaned
),
summarized_rows AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        (SELECT COUNT(CASE WHEN t.actor NOT IN ('system', 'customer') THEN 1 END) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS event_actor_count,
        (SELECT COUNT(*) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS event_count,
        (SELECT MAX(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS last_event_at
    FROM enriched s
),
ticket_id_stats AS (
    -- Median event_count per ticket_id
    SELECT ticket_id, PERCENTILE_APPROX(event_count, 0.5) AS median_event_count
    FROM summarized_rows
    GROUP BY ticket_id
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_event_count,
        ROUND(s.event_count - g.median_event_count, 2) AS event_count_minus_median
    FROM summarized_rows s
    JOIN ticket_id_stats g ON g.ticket_id = s.ticket_id
    WHERE s.event_count >= g.median_event_count
),
exploded_detail AS (
    -- One row per event
    SELECT
        s.id,
        s.ticket_id,
        s.actor,
        s.event_type_actor_label,
        s.event_actor_count,
        s.median_event_count,
        t.id AS event_id,
        t.event_type AS event_event_type,
        t.event_at AS event_event_at,
        t.actor AS event_actor
    FROM above_average s
    LEFT JOIN ticket_events t ON t.ticket_id = s.ticket_id
),
qualified AS (
    -- Rows with no matching ticket_events
    SELECT s.*
    FROM exploded_detail s
    WHERE NOT EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.ticket_id
              AND t.event_type <> 'resolved'
              AND MONTH(t.event_at) IN (1, 2)
        )
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT event_actor, event_actor_count, actor, median_event_count
    FROM qualified
) src
PIVOT (
    SUM(median_event_count)
    FOR actor IN ('customer', 'agent')
)
ORDER BY event_actor, event_actor_count
