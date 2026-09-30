WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATE_FORMAT(event_at, 'MMM yyyy') AS event_at_label
    FROM ticket_events
    WHERE event_type = 'replied'
),
with_prev AS (
    -- Previous event_at per actor
    SELECT *, LAG(event_at) OVER (PARTITION BY actor ORDER BY event_at, id) AS prev_event_at
    FROM base
),
flagged AS (
    -- Flag rows that start a new session (gap > 90 minutes)
    SELECT *, CASE WHEN prev_event_at IS NULL OR (UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(prev_event_at)) / 60 > 90 THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY actor ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
session_summary AS (
    -- One row per session
    SELECT
        actor,
        burst_id,
        MIN(event_at) AS session_start,
        MAX(event_at) AS session_end,
        COUNT(*) AS event_count,
        ROUND((UNIX_TIMESTAMP(MAX(event_at)) - UNIX_TIMESTAMP(MIN(event_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY actor, burst_id
),
rollup_base AS (
    -- Group-level totals
    SELECT
        actor,
        SUM(CASE WHEN actor IS NOT NULL THEN event_count ELSE 0 END) AS event_count_actor_is_not_null,
        COUNT(*) AS row_count
    FROM session_summary
    WHERE duration_min < 192.0
    GROUP BY actor
    HAVING COUNT(*) >= 2
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        actor,
        DENSE_RANK() OVER (ORDER BY event_count_actor_is_not_null DESC, actor) AS event_count_actor_is_not_null_dense_rank,
        NTH_VALUE(row_count, 2) OVER (ORDER BY event_count_actor_is_not_null DESC, actor ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_row_count,
        NTILE(4) OVER w_all_by_event_count_actor_is AS event_count_actor_is_not_null_quartile,
        LAG(row_count) OVER (ORDER BY event_count_actor_is_not_null DESC, actor) AS prev_row_count,
        LAG(row_count, 1, 0) OVER w_all_by_event_count_actor_is AS prev_row_count_2,
        LAG(row_count, 2) OVER (ORDER BY event_count_actor_is_not_null DESC, actor) AS prev2_row_count
    FROM rollup_base
    WINDOW w_all_by_event_count_actor_is AS (ORDER BY event_count_actor_is_not_null DESC, actor)
)
SELECT
    actor,
    event_count_actor_is_not_null_dense_rank,
    prev_row_count_2,
    prev2_row_count,
    COALESCE(actor, 'UNKNOWN') AS actor_or_unknown
FROM ranked
ORDER BY actor
