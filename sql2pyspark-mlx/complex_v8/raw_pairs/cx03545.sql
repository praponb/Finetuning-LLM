WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        QUARTER(event_at) AS event_at_quarter,
        COALESCE(actor, 'UNKNOWN') AS actor_or_unknown,
        event_at + INTERVAL 15 MINUTES AS event_at_plus_15m
    FROM ticket_events
    WHERE (ticket_id >= 45 OR event_at < TIMESTAMP '2025-02-24 10:00:00')
),
summary AS (
    -- Group-level totals
    SELECT
        event_type,
        SUM(event_at_quarter) AS total_event_at_quarter,
        ARRAY_SORT(COLLECT_SET(actor)) AS actor_list,
        COUNT(DISTINCT ticket_id) AS distinct_ticket_ids
    FROM filtered
    WHERE actor_or_unknown <> 'customer'
    GROUP BY 1
    HAVING SUM(event_at_quarter) > 2
),
qualified AS (
    -- Compare against the overall min
    SELECT *
    FROM summary
    WHERE distinct_ticket_ids > (SELECT MIN(distinct_ticket_ids) FROM summary)
),
qualified_2 AS (
    -- Compare against the overall avg
    SELECT *
    FROM qualified
    WHERE total_event_at_quarter > (SELECT AVG(total_event_at_quarter) FROM qualified)
)
SELECT event_type, total_event_at_quarter, actor_list
FROM qualified_2
ORDER BY total_event_at_quarter
