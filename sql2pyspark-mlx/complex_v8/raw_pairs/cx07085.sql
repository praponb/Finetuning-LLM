WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        LOWER(REGEXP_REPLACE(TRIM(event_type), ' ', '_')) AS event_type_slug,
        CONCAT(event_type, '#', CAST(ticket_id AS STRING)) AS event_type_ref,
        SUBSTRING(TRIM(event_type), 1, 4) AS event_type_prefix
    FROM ticket_events
    WHERE (ticket_id <= 24 OR event_at >= TIMESTAMP '2025-01-25 07:00:00')
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        ticket_id,
        event_type,
        actor,
        event_type_slug,
        event_type_ref,
        ROUND(CUME_DIST() OVER (PARTITION BY event_type ORDER BY event_at DESC), 3) AS event_at_cume_dist,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER (PARTITION BY event_type ORDER BY event_at DESC, id))) / 60, 1) AS minutes_since_prev_event_at,
        LAG(event_at, 2) OVER w_event_type_by_event_at AS prev2_event_at,
        LAST_VALUE(event_at) OVER (PARTITION BY event_type ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_event_at
    FROM valid
    WINDOW w_event_type_by_event_at AS (PARTITION BY event_type ORDER BY event_at DESC, id)
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, metric, reading
FROM (
    SELECT id, event_at_cume_dist, minutes_since_prev_event_at
    FROM sequenced
) src
UNPIVOT (
    reading FOR metric IN (event_at_cume_dist, minutes_since_prev_event_at)
) unp
ORDER BY id, metric
