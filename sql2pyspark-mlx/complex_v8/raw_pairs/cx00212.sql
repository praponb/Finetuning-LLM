WITH cleaned_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE MONTH(event_at) IN (1, 2)
),
totals AS (
    -- Group-level totals
    SELECT
        ticket_id,
        event_type,
        actor,
        ARRAY_SORT(COLLECT_SET(event_type)) AS event_type_list,
        COUNT(*) AS n_rows,
        COUNT(DISTINCT event_type) AS distinct_event_types,
        COUNT(*) FILTER (WHERE event_type NOT IN ('assigned', 'reopened')) AS n_event_type_not_in_assigned_r
    FROM cleaned_events
    WHERE ticket_id IS NOT NULL
    GROUP BY ticket_id, event_type, actor
),
without_matches AS (
    -- Rows with no matching ticket_events
    SELECT s.*
    FROM totals s
    WHERE NOT EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.ticket_id
              AND t.event_at < TIMESTAMP '2025-03-02 09:00:00'
              AND t.event_type NOT IN ('assigned', 'created')
        )
),
event_stats AS (
    -- Per-ticket ticket_events metrics
    SELECT ticket_id, MAX(event_at) AS last_event_at
    FROM ticket_events
    GROUP BY ticket_id
),
with_counts AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        t.last_event_at
    FROM without_matches s
    LEFT JOIN event_stats t ON t.ticket_id = s.ticket_id
),
annotated AS (
    -- Derive helper columns
    SELECT
        ticket_id,
        event_type,
        actor,
        event_type_list,
        n_rows,
        n_event_type_not_in_assigned_r,
        last_event_at,
        SUBSTRING(TRIM(actor), 1, 2) AS actor_prefix,
        CONCAT(event_type, '#', CAST(ticket_id AS STRING)) AS event_type_ref,
        COALESCE(distinct_event_types, 0) AS distinct_event_types_filled
    FROM with_counts
    WHERE ticket_id IS NOT NULL
),
enriched_ticket AS (
    -- Enrich with dimension data
    SELECT
        s.ticket_id,
        s.event_type,
        s.actor,
        s.event_type_list,
        s.n_rows,
        s.last_event_at,
        s.distinct_event_types_filled,
        t.closed_at AS ticket_closed_at
    FROM annotated s
    JOIN tickets t ON t.id = s.ticket_id
)
SELECT
    COALESCE(event_type, 'ALL') AS event_type,
    n_rows,
    ticket_id,
    MIN(ticket_closed_at) AS first_ticket_closed_at,
    ARRAY_SORT(COLLECT_SET(event_type)) AS event_type_list_2
FROM enriched_ticket
GROUP BY GROUPING SETS ((event_type, n_rows, ticket_id), (event_type), ())
ORDER BY event_type NULLS LAST, n_rows NULLS LAST, ticket_id NULLS LAST
