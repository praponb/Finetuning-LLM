WITH recent AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CONCAT(actor, '#', CAST(ticket_id AS STRING)) AS actor_ref
    FROM ticket_events
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        YEAR(event_at) AS event_at_year,
        COUNT(*) FILTER (WHERE event_type NOT IN ('assigned', 'reopened')) AS n_event_type_not_in_assigned_r,
        COUNT(*) AS cnt,
        ARRAY_SORT(COLLECT_SET(event_type)) AS event_type_list,
        MIN(event_at) AS first_event_at
    FROM recent
    GROUP BY YEAR(event_at)
    HAVING COUNT(*) >= 2
),
filtered AS (
    -- Compare against the overall avg
    SELECT *
    FROM totals
    WHERE cnt > (SELECT AVG(cnt) FROM totals)
)
-- Final projection
SELECT
    event_at_year,
    n_event_type_not_in_assigned_r,
    cnt,
    event_type_list,
    SIZE(event_type_list) AS event_type_list_count
FROM filtered
ORDER BY event_at_year
