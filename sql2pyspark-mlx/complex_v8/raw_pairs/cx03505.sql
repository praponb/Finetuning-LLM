WITH cleaned AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATEDIFF(DATE '2026-01-01', event_at) AS days_since_event_at,
        DATE_FORMAT(event_at, 'yyyy-MM') AS event_at_label
    FROM ticket_events
    WHERE actor <> 'agent'
      AND event_at >= TIMESTAMP '2025-02-06 10:00:00'
),
filtered AS (
    -- Rows that have matching ticket_events
    SELECT s.*
    FROM cleaned s
    WHERE EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.ticket_id
              AND HOUR(t.event_at) BETWEEN 9 AND 15
        )
),
event_stats AS (
    -- Per-ticket ticket_events metrics
    SELECT
        ticket_id,
        MAX(event_at) AS last_event_at,
        MIN(event_at) AS first_event_at
    FROM ticket_events
    GROUP BY ticket_id
),
with_stats AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        t.last_event_at,
        t.first_event_at
    FROM filtered s
    LEFT JOIN event_stats t ON t.ticket_id = s.ticket_id
),
latest AS (
    -- Keep the earliest row per ticket_id, event_type
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        days_since_event_at,
        event_at_label,
        last_event_at,
        first_event_at
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY ticket_id, event_type ORDER BY days_since_event_at, id) AS recency_rank
        FROM with_stats
    ) ranked
    WHERE recency_rank = 1
)
SELECT
    actor,
    event_type,
    GROUPING_ID() AS gid,
    ROUND(100 * COUNT(CASE WHEN event_type = 'replied' THEN 1 END) / COUNT(*), 1) AS pct_event_type_replied
FROM latest
GROUP BY ROLLUP(actor, event_type)
