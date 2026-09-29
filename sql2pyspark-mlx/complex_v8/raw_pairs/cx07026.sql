WITH filtered AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CONCAT(actor, '#', CAST(id AS STRING)) AS actor_ref,
        COALESCE(event_type, 'UNKNOWN') AS event_type_or_unknown
    FROM ticket_events
    WHERE TO_DATE(event_at) < DATE '2025-02-24'
      AND event_type IN ('replied', 'assigned')
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        id,
        event_type,
        actor,
        NTILE(10) OVER (ORDER BY event_at DESC NULLS FIRST, id) AS event_at_decile,
        LEAD(event_at, 3) OVER (ORDER BY event_at DESC NULLS FIRST, id) AS next3_event_at,
        FIRST_VALUE(event_at) OVER (ORDER BY event_at DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_event_at,
        NTILE(3) OVER (ORDER BY event_at DESC NULLS FIRST, id) AS event_at_tercile
    FROM filtered
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        id,
        event_type,
        actor,
        next3_event_at,
        event_at_tercile,
        LAST_VALUE(first_event_at) IGNORE NULLS OVER (PARTITION BY actor ORDER BY first_event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_first_event_at,
        COUNT(*) OVER w_event_type_actor AS rows_in_event_type_actor,
        ROUND(AVG(event_at_tercile) OVER (ORDER BY next3_event_at ASC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS event_at_tercile_avg_3_prior,
        FIRST_VALUE(event_at_decile) IGNORE NULLS OVER (PARTITION BY actor ORDER BY first_event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_event_at_decile
    FROM with_windows
    WINDOW w_event_type_actor AS (PARTITION BY event_type, actor)
)
-- Subtotals with GROUPING SETS
SELECT
    COALESCE(actor, 'ALL') AS actor,
    COALESCE(event_type, 'ALL') AS event_type,
    event_at_tercile,
    SUM(first_known_event_at_decile) AS total_first_known_event_at_decile,
    ROUND(SUM(rows_in_event_type_actor) / NULLIF(SUM(event_at_tercile_avg_3_prior), 0), 3) AS rows_in_event_type_actor_to_event_at_tercile_avg
FROM with_trends
GROUP BY GROUPING SETS ((actor, event_type, event_at_tercile), (actor))
