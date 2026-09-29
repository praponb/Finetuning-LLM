WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CASE WHEN event_type IN ('resolved', 'reopened') THEN 'group_a' WHEN event_type = 'created' THEN 'group_b' ELSE 'other' END AS event_type_group
    FROM ticket_events
    WHERE event_type = 'assigned'
),
filtered_2 AS (
    -- Rows that have matching ticket_events
    SELECT s.*
    FROM scoped s
    WHERE s.ticket_id IN (
            SELECT t.ticket_id
            FROM ticket_events t
            WHERE t.actor IN ('system', 'customer')
        )
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(event_at) OVER (PARTITION BY ticket_id, event_type ORDER BY event_at DESC, id) AS next_event_at,
        DENSE_RANK() OVER (PARTITION BY event_type ORDER BY event_at) AS event_at_dense_rank,
        LAG(event_at) OVER (PARTITION BY event_type ORDER BY event_at, id) AS prev_event_at
    FROM filtered_2
)
-- Subtotals with ROLLUP
SELECT
    COALESCE(event_type_group, 'ALL') AS event_type_group,
    COALESCE(event_type, 'ALL') AS event_type,
    ticket_id,
    GROUPING_ID() AS gid,
    SUM(event_at_dense_rank) AS total_event_at_dense_rank,
    ROUND(AVG(event_at_dense_rank), 2) AS avg_event_at_dense_rank
FROM sequenced
GROUP BY ROLLUP(event_type_group, event_type, ticket_id)
ORDER BY event_type_group NULLS LAST, event_type NULLS LAST, ticket_id NULLS LAST
