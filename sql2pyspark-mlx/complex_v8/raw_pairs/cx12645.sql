WITH valid AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATEDIFF(DATE '2026-01-01', event_at) AS days_since_event_at,
        SUBSTRING(TRIM(event_type), 1, 4) AS event_type_prefix
    FROM ticket_events
),
screened AS (
    -- Rows with no matching ticket_events
    SELECT s.*
    FROM valid s
    WHERE NOT EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.ticket_id
              AND t.event_type = 'resolved'
        )
),
filtered AS (
    -- Rows that have matching ticket_events
    SELECT s.*
    FROM screened s
    WHERE s.ticket_id IN (
            SELECT t.ticket_id
            FROM ticket_events t
        )
),
event_stats AS (
    -- Per-ticket ticket_events metrics
    SELECT
        ticket_id,
        COUNT(CASE WHEN actor <> 'customer' THEN 1 END) AS event_actor_count
    FROM ticket_events
    GROUP BY ticket_id
),
with_counts AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        COALESCE(t.event_actor_count, 0) AS event_actor_count
    FROM filtered s
    LEFT JOIN event_stats t ON t.ticket_id = s.ticket_id
),
rollup_base AS (
    -- Summarise per group
    SELECT
        event_actor_count,
        event_type,
        actor,
        COUNT(DISTINCT id) AS distinct_ids,
        COUNT(DISTINCT event_type) AS distinct_event_types,
        COUNT(*) AS n_rows
    FROM with_counts
    WHERE event_type_prefix IS NOT NULL
    GROUP BY 1, 2, 3
    HAVING MAX(days_since_event_at) > 70
),
filtered_2 AS (
    -- Compare against the overall avg
    SELECT *
    FROM rollup_base
    WHERE n_rows > (SELECT AVG(n_rows) FROM rollup_base)
)
-- Final projection
SELECT
    event_actor_count,
    event_type,
    actor,
    distinct_event_types,
    n_rows
FROM filtered_2
WHERE event_type = 'resolved'
