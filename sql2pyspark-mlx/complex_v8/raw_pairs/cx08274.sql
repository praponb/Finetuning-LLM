WITH base_events AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        LOWER(TRIM(event_type)) AS event_type_clean
    FROM ticket_events
    WHERE event_type NOT IN ('resolved', 'escalated')
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        RANK() OVER (PARTITION BY event_type, actor ORDER BY event_at ASC, id) AS event_at_rank,
        NTILE(3) OVER (PARTITION BY event_type, actor ORDER BY event_at ASC, id) AS event_at_tercile,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER w_event_type_actor_by_event_at)) / 60, 1) AS minutes_since_prev_event_at,
        FIRST_VALUE(event_at) OVER (PARTITION BY event_type, actor ORDER BY event_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_event_at
    FROM base_events
    WINDOW w_event_type_actor_by_event_at AS (PARTITION BY event_type, actor ORDER BY event_at ASC, id)
)
SELECT
    COALESCE(actor, 'ALL') AS actor,
    COALESCE(event_type, 'ALL') AS event_type,
    COUNT(DISTINCT id) AS distinct_ids,
    ROUND(SUM(minutes_since_prev_event_at) / NULLIF(SUM(event_at_rank), 0), 3) AS minutes_since_prev_event_at_to_event_at_rank_rat
FROM windowed
GROUP BY GROUPING SETS ((actor, event_type), (actor), (event_type), ())
ORDER BY actor NULLS LAST, event_type NULLS LAST
