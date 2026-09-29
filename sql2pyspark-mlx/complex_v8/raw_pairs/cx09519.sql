WITH cleaned_events AS (
    -- Scope the ticket_events rows
    SELECT *
    FROM ticket_events
    WHERE TO_DATE(event_at) < DATE '2025-03-02'
),
with_prev AS (
    -- Previous event_at per actor
    SELECT *, LAG(event_at) OVER (PARTITION BY actor ORDER BY event_at, id) AS prev_event_at
    FROM cleaned_events
),
flagged AS (
    -- Flag rows that start a new session (gap > 45 minutes)
    SELECT *, CASE WHEN prev_event_at IS NULL OR (UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(prev_event_at)) / 60 > 45 THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY actor ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
per_session AS (
    -- One row per session
    SELECT
        actor,
        session_id,
        MIN(event_at) AS session_start,
        MAX(event_at) AS session_end,
        COUNT(*) AS events,
        ROUND((UNIX_TIMESTAMP(MAX(event_at)) - UNIX_TIMESTAMP(MIN(event_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY actor, session_id
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        MIN(session_id) OVER (ORDER BY session_end ASC, actor, session_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_session_id_so_far,
        ROUND(PERCENT_RANK() OVER (ORDER BY session_start), 3) AS session_start_percent_rank,
        ROUND(AVG(duration_min) OVER (ORDER BY session_start, actor, session_id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS duration_min_avg_2,
        ROUND(AVG(duration_min) OVER (ORDER BY session_start, actor, session_id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS duration_min_avg_7,
        ROUND(AVG(session_id) OVER (PARTITION BY session_id, actor), 2) AS session_id_actor_avg_session_id,
        LAST_VALUE(session_start) IGNORE NULLS OVER (ORDER BY session_start, actor, session_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_session_start
    FROM per_session
)
SELECT
    actor,
    session_id,
    session_start,
    events,
    min_session_id_so_far,
    session_start_percent_rank,
    duration_min_avg_2,
    session_id_actor_avg_session_id,
    last_known_session_start,
    CONCAT(actor, '#', CAST(events AS STRING)) AS actor_ref,
    LOWER(REGEXP_REPLACE(TRIM(actor), ' ', '_')) AS actor_slug
FROM sequenced
WHERE session_id_actor_avg_session_id <= 5.2
ORDER BY session_id DESC
