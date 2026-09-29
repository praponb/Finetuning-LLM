WITH recent_events AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE actor IN ('system', 'agent')
      AND event_type IN ('replied', 'assigned')
),
unique_rows AS (
    -- Keep the latest row per event_type, ticket_id
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY event_type, ticket_id ORDER BY event_at DESC, id DESC) AS recency_rank
        FROM recent_events
    ) ranked
    WHERE recency_rank = 1
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        NTH_VALUE(event_at, 2) OVER (PARTITION BY DATE_TRUNC('MONTH', event_at), ticket_id ORDER BY event_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at,
        RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', event_at), ticket_id ORDER BY event_at NULLS LAST) AS event_at_rank
    FROM unique_rows
),
filtered AS (
    -- Rows with no matching ticket_events
    SELECT s.*
    FROM sequenced s
    WHERE NOT EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.ticket_id
              AND t.actor = 'agent'
        )
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        event_type,
        DATE_TRUNC('MONTH', event_at) AS event_at_month,
        ticket_id,
        ARRAY_SORT(COLLECT_SET(event_type)) AS event_type_list,
        SUM(event_at_rank) AS total_event_at_rank
    FROM filtered
    GROUP BY 1, 2, 3
)
-- Final projection
SELECT
    event_type,
    event_at_month,
    ticket_id,
    event_type_list
FROM totals
WHERE HOUR(event_at_month) BETWEEN 10 AND 16
ORDER BY event_at_month DESC NULLS FIRST, ticket_id DESC NULLS FIRST, event_type
LIMIT 10
