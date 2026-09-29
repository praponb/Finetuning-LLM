WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATE_FORMAT(event_at, 'yyyy-MM') AS event_at_label,
        DATE_TRUNC('QUARTER', event_at) AS event_at_quarter
    FROM ticket_events
    WHERE actor <> 'agent'
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LAST_VALUE(event_at_quarter) OVER (PARTITION BY YEAR(event_at_quarter) ORDER BY event_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_event_at_quarter,
        NTH_VALUE(event_at, 2) OVER (PARTITION BY event_type, ticket_id ORDER BY event_at_quarter ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at
    FROM filtered
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(CUME_DIST() OVER w_ticket_id_actor_by_event_at, 3) AS event_at_quarter_cume_dist,
        ROUND(CUME_DIST() OVER (PARTITION BY MONTH(second_event_at) ORDER BY event_at_quarter), 3) AS event_at_quarter_cume_dist_2
    FROM with_windows
    WINDOW w_ticket_id_actor_by_event_at AS (PARTITION BY ticket_id, actor ORDER BY event_at_quarter ASC NULLS LAST)
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_at,
    actor,
    event_at_quarter,
    last_event_at_quarter,
    second_event_at,
    event_at_quarter_cume_dist
FROM windowed
WHERE event_at >= TIMESTAMP '2025-02-06 10:00:00'
