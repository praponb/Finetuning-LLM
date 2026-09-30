WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        LOWER(REGEXP_REPLACE(TRIM(event_type), ' ', '_')) AS event_type_slug,
        CONCAT(event_type, '#', CAST(ticket_id AS STRING)) AS event_type_ref
    FROM ticket_events
    WHERE HOUR(event_at) BETWEEN 9 AND 15
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        event_type_ref,
        ROUND(CUME_DIST() OVER (PARTITION BY ticket_id, event_type ORDER BY event_at DESC NULLS FIRST), 3) AS event_at_cume_dist,
        NTH_VALUE(event_at, 2) OVER (PARTITION BY ticket_id ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at,
        LAST_VALUE(event_at) OVER (PARTITION BY ticket_id ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_event_at,
        ROW_NUMBER() OVER (PARTITION BY event_type, ticket_id ORDER BY event_at, id) AS rn,
        LAG(event_at, 2) OVER (PARTITION BY event_type, ticket_id ORDER BY event_at, id) AS prev2_event_at
    FROM prepared
)
-- Subtotals with GROUPING SETS
SELECT
    COALESCE(event_type, 'ALL') AS event_type,
    ticket_id,
    rn,
    GROUPING(event_type) AS is_total_event_type,
    SUM(event_at_cume_dist) AS total_event_at_cume_dist,
    COUNT(*) AS cnt,
    SUM(rn) FILTER (WHERE actor IN ('customer', 'agent')) AS rn_actor_in_customer_agent
FROM metrics
GROUP BY GROUPING SETS ((event_type, ticket_id, rn), (event_type), (ticket_id), ())
ORDER BY event_type NULLS LAST, ticket_id NULLS LAST, rn NULLS LAST
