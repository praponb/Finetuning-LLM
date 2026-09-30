WITH valid AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATE_TRUNC('YEAR', event_at) AS event_at_year,
        SUBSTRING(TRIM(event_type), 1, 4) AS event_type_prefix
    FROM ticket_events
    WHERE event_at >= TIMESTAMP '2025-01-25 09:00:00'
      AND actor <> 'system'
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(CUME_DIST() OVER w_event_at_to_date_by_event_at, 3) AS event_at_cume_dist,
        ROW_NUMBER() OVER (PARTITION BY TO_DATE(event_at) ORDER BY event_at, id) AS seq_no,
        LAG(event_at) OVER (PARTITION BY TO_DATE(event_at) ORDER BY event_at, id) AS prev_event_at,
        LAG(event_at_year, 2) OVER (PARTITION BY TO_DATE(event_at) ORDER BY event_at, id) AS prev2_event_at_year
    FROM valid
    WINDOW w_event_at_to_date_by_event_at AS (PARTITION BY TO_DATE(event_at) ORDER BY event_at, id)
),
top_per_group AS (
    -- First row per group
    SELECT *
    FROM windowed
    WHERE seq_no = 1
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT actor, event_at, event_type, event_at_cume_dist
    FROM top_per_group
) src
PIVOT (
    MAX(event_at_cume_dist) AS max_v, COUNT(event_at_cume_dist) AS n
    FOR event_type IN ('replied', 'resolved')
)
ORDER BY actor, event_at
