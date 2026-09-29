WITH base_events AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATE_FORMAT(event_at, 'yyyy-MM-dd HH:00') AS event_at_label,
        MONTH(event_at) AS event_at_month
    FROM ticket_events
    WHERE event_type IN ('assigned', 'reopened')
      AND ticket_id IS NOT NULL
),
event_stats AS (
    -- Per-ticket ticket_events metrics
    SELECT
        ticket_id,
        COUNT(*) AS event_count,
        MIN(event_at) AS first_event_at,
        COUNT(*) AS event_count_2
    FROM ticket_events
    GROUP BY ticket_id
),
with_counts AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        COALESCE(t.event_count, 0) AS event_count,
        t.first_event_at,
        COALESCE(t.event_count_2, 0) AS event_count_2
    FROM base_events s
    LEFT JOIN event_stats t ON t.ticket_id = s.ticket_id
),
tagged AS (
    -- Label and score rows
    SELECT
        id,
        event_at,
        event_type,
        actor,
        event_at_label,
        event_at_month,
        event_count,
        first_event_at,
        event_count_2,
        SUBSTRING(TRIM(actor), 1, 2) AS actor_prefix
    FROM with_counts
    WHERE event_at_label IS NOT NULL
),
without_matches AS (
    -- Compare against the overall avg
    SELECT *
    FROM tagged
    WHERE event_count_2 > (SELECT AVG(event_count_2) FROM tagged)
),
labeled AS (
    -- Label and score rows
    SELECT *, COALESCE(event_type, 'UNKNOWN') AS event_type_or_unknown
    FROM without_matches
    WHERE event_at_month <= 10
)
-- Final projection
SELECT
    id,
    event_at,
    event_type,
    event_at_label,
    event_at_month,
    event_count,
    first_event_at,
    event_type_or_unknown,
    DATE_FORMAT(event_at, 'yyyy-MM') AS event_at_label_2
FROM labeled
ORDER BY event_at_month DESC, id
LIMIT 20
