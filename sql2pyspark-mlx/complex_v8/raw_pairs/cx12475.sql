WITH prepared_events AS (
    -- Filter ticket_events
    SELECT *
    FROM ticket_events
    WHERE (actor NOT IN ('customer', 'system') OR event_at < TIMESTAMP '2025-02-24 08:00:00')
),
screened AS (
    -- Rows that have matching ticket_events
    SELECT s.*
    FROM prepared_events s
    WHERE EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.ticket_id
        )
),
scored AS (
    -- Add calculated columns
    SELECT
        *,
        CONCAT(event_type, '#', CAST(ticket_id AS STRING)) AS event_type_ref,
        UPPER(TRIM(event_type)) AS event_type_clean
    FROM screened
),
event_stats AS (
    -- Per-ticket ticket_events metrics
    SELECT ticket_id, MIN(event_at) AS first_event_at
    FROM ticket_events
    GROUP BY ticket_id
),
with_counts AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        t.first_event_at
    FROM scored s
    LEFT JOIN event_stats t ON t.ticket_id = s.ticket_id
)
-- Subtotals with GROUPING SETS
SELECT
    actor,
    event_type,
    ROUND(100 * COUNT(CASE WHEN event_type = 'escalated' THEN 1 END) / COUNT(*), 1) AS pct_event_type_escalated,
    COUNT(*) FILTER (WHERE actor = 'agent') AS n_actor_agent,
    COUNT(*) FILTER (WHERE event_type NOT IN ('replied', 'resolved')) AS n_event_type_not_in_replied_re
FROM with_counts
GROUP BY GROUPING SETS ((actor, event_type), (actor), ())
ORDER BY actor NULLS LAST, event_type NULLS LAST
