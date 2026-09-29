WITH recent AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE event_type NOT IN ('assigned', 'escalated')
      AND ticket_id IS NOT NULL
),
enriched AS (
    -- Label and score rows
    SELECT
        id,
        ticket_id,
        event_type,
        DATE_FORMAT(event_at, 'MMM yyyy') AS event_at_label,
        UPPER(TRIM(event_type)) AS event_type_clean
    FROM recent
),
totals AS (
    -- Summarise per group
    SELECT
        ticket_id,
        COUNT(*) AS cnt,
        COUNT(DISTINCT ticket_id) AS distinct_ticket_ids,
        ARRAY_SORT(COLLECT_SET(event_type)) AS event_type_list,
        COUNT(*) FILTER (WHERE event_type NOT IN ('replied', 'assigned')) AS n_event_type_not_in_replied_as,
        COUNT(*) FILTER (WHERE event_type IS NOT NULL) AS n_event_type_is_not_null
    FROM enriched
    WHERE event_type = 'created'
    GROUP BY ticket_id
),
with_activity AS (
    -- Rows with no matching ticket_events
    SELECT s.*
    FROM totals s
    WHERE NOT EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.ticket_id
              AND t.event_at >= TIMESTAMP '2025-01-25 09:00:00'
        )
)
-- Final projection
SELECT
    ticket_id,
    cnt,
    distinct_ticket_ids,
    n_event_type_not_in_replied_as,
    n_event_type_is_not_null
FROM with_activity
ORDER BY n_event_type_is_not_null NULLS LAST, n_event_type_not_in_replied_as DESC
