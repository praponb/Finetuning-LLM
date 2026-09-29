WITH cleaned AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        event_at + INTERVAL 30 MINUTES AS event_at_plus_30m,
        DATE_FORMAT(event_at, 'yyyy-MM-dd HH:00') AS event_at_label,
        LENGTH(TRIM(event_type)) AS event_type_len
    FROM ticket_events
    WHERE event_type IN ('replied', 'created')
),
latest_per_key AS (
    -- Keep the latest row per ticket_id
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        event_at_plus_30m,
        event_at_label,
        event_type_len
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY ticket_id ORDER BY event_type_len DESC, id DESC) AS row_num
        FROM cleaned
    ) ranked
    WHERE row_num = 1
),
without_matches AS (
    -- Rows with no matching ticket_events
    SELECT s.*
    FROM latest_per_key s
    WHERE NOT EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.ticket_id
              AND t.actor IS NOT NULL
              AND t.event_type IN ('resolved', 'replied', 'escalated')
        )
)
SELECT
    event_type,
    ticket_id,
    MIN(event_type_len) AS min_event_type_len
FROM without_matches
GROUP BY ROLLUP(event_type, ticket_id)
ORDER BY event_type NULLS LAST, ticket_id NULLS LAST
