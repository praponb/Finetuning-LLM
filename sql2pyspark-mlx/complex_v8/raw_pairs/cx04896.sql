WITH scoped_events AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        HOUR(event_at) AS event_at_hour
    FROM ticket_events
),
with_prev AS (
    -- Previous event_at per actor
    SELECT *, LAG(event_at) OVER (PARTITION BY actor ORDER BY event_at, id) AS prev_event_at
    FROM scoped_events
),
flagged AS (
    -- Flag rows that start a new session (gap > 60 minutes)
    SELECT *, CASE WHEN prev_event_at IS NULL OR event_at > prev_event_at + INTERVAL 60 MINUTES THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY actor ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
sessions AS (
    -- One row per session
    SELECT
        actor,
        burst_id,
        MIN(event_at) AS session_start,
        MAX(event_at) AS session_end,
        COUNT(*) AS events,
        ROUND((UNIX_TIMESTAMP(MAX(event_at)) - UNIX_TIMESTAMP(MIN(event_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY actor, burst_id
),
rollup_base AS (
    -- Aggregate with conditional metrics
    SELECT
        TO_DATE(session_end) AS session_end_day,
        COUNT(DISTINCT actor) AS distinct_actors,
        ARRAY_SORT(COLLECT_SET(actor)) AS actor_list,
        COUNT(*) AS n_rows
    FROM sessions
    GROUP BY TO_DATE(session_end)
    HAVING MAX(burst_id) > 2
)
-- Subtotals with GROUPING SETS
SELECT
    distinct_actors,
    n_rows,
    GROUPING(distinct_actors) AS is_total_distinct_actors,
    ROUND(SUM(n_rows) / NULLIF(SUM(distinct_actors), 0), 3) AS n_rows_to_distinct_actors_ratio,
    MIN(session_end_day) AS first_session_end_day,
    MIN(distinct_actors) AS min_distinct_actors
FROM rollup_base
GROUP BY GROUPING SETS ((distinct_actors, n_rows), (distinct_actors), (n_rows))
ORDER BY distinct_actors NULLS LAST, n_rows NULLS LAST
