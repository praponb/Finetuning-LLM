WITH filtered_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        event_at + INTERVAL 2 HOURS AS event_at_plus_2h,
        DATE_FORMAT(event_at, 'yyyy-MM-dd') AS event_at_label
    FROM ticket_events
    WHERE (actor IS NOT NULL OR MONTH(event_at) IN (1, 3))
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        YEAR(event_at_plus_2h) AS event_at_plus_2h_year,
        ticket_id,
        COUNT(*) AS n_rows,
        ROUND(100 * COUNT(CASE WHEN event_type = 'resolved' THEN 1 END) / COUNT(*), 1) AS pct_event_type_resolved,
        ARRAY_SORT(COLLECT_SET(actor)) AS actor_list,
        COUNT(DISTINCT id) AS distinct_ids,
        COUNT(*) FILTER (WHERE event_type IN ('created', 'resolved')) AS n_event_type_in_created_resolv,
        COUNT(DISTINCT event_type) AS distinct_event_types
    FROM filtered_events
    GROUP BY 1, 2
),
screened AS (
    -- Rows that have matching ticket_events
    SELECT s.*
    FROM totals s
    WHERE s.ticket_id IN (
            SELECT t.ticket_id
            FROM ticket_events t
        )
),
scored AS (
    -- Label and score rows
    SELECT
        *,
        COALESCE(pct_event_type_resolved, 0.0) AS pct_event_type_resolved_filled,
        CASE WHEN distinct_event_types >= 7 THEN 'large' WHEN distinct_event_types >= 5 THEN 'medium' ELSE 'small' END AS distinct_event_types_tier,
        event_at_plus_2h_year - distinct_ids AS event_at_plus_2h_year_minus_distinct_ids
    FROM screened
    WHERE distinct_ids >= 4
),
enriched_ticket AS (
    -- Join tickets
    SELECT s.*, t.channel AS ticket_channel, t.closed_at AS ticket_closed_at
    FROM scored s
    JOIN tickets t ON t.id = s.ticket_id
    WHERE t.channel IS NOT NULL
),
event_stats AS (
    -- Per-ticket ticket_events metrics
    SELECT ticket_id, MIN(event_at) AS first_event_at
    FROM ticket_events
    GROUP BY ticket_id
),
with_stats AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        t.first_event_at
    FROM enriched_ticket s
    LEFT JOIN event_stats t ON t.ticket_id = s.ticket_id
)
-- Native SQL UNPIVOT (wide to long)
SELECT event_at_plus_2h_year, ticket_id, measure, reading
FROM (
    SELECT event_at_plus_2h_year, ticket_id, pct_event_type_resolved, pct_event_type_resolved_filled
    FROM with_stats
) src
UNPIVOT (
    reading FOR measure IN (pct_event_type_resolved, pct_event_type_resolved_filled)
) unp
ORDER BY event_at_plus_2h_year, ticket_id, measure
