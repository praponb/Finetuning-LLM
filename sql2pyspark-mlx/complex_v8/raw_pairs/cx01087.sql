WITH scoped_tickets AS (
    -- Scope the tickets rows
    SELECT *
    FROM tickets
    WHERE requester_id < 20
),
with_prev AS (
    -- Previous opened_at per agent_id
    SELECT *, LAG(opened_at) OVER (PARTITION BY agent_id ORDER BY opened_at, id) AS prev_opened_at
    FROM scoped_tickets
),
flagged AS (
    -- Flag rows that start a new session (gap > 45 minutes)
    SELECT *, CASE WHEN prev_opened_at IS NULL OR (UNIX_TIMESTAMP(opened_at) - UNIX_TIMESTAMP(prev_opened_at)) / 60 > 45 THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY agent_id ORDER BY opened_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
per_session AS (
    -- One row per session
    SELECT
        agent_id,
        session_id,
        MIN(opened_at) AS session_start,
        MAX(opened_at) AS session_end,
        COUNT(*) AS event_count,
        SUM(requester_id) AS session_requester_id,
        ROUND((UNIX_TIMESTAMP(MAX(opened_at)) - UNIX_TIMESTAMP(MIN(opened_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY agent_id, session_id
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        session_id,
        ROUND(STDDEV(event_count), 2) AS stddev_event_count,
        COUNT(DISTINCT agent_id) AS distinct_agent_ids,
        MIN(duration_min) AS min_duration_min,
        SUM(session_id) AS total_session_id,
        MAX(session_id) AS max_session_id
    FROM per_session
    GROUP BY 1
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        session_id,
        distinct_agent_ids,
        total_session_id,
        max_session_id,
        COUNT(*) OVER (PARTITION BY session_id) AS rows_in_session_id,
        FIRST_VALUE(distinct_agent_ids) IGNORE NULLS OVER (PARTITION BY session_id, max_session_id ORDER BY total_session_id ASC NULLS LAST, session_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_distinct_agent_ids
    FROM stats
)
-- Final projection
SELECT
    session_id,
    distinct_agent_ids,
    max_session_id,
    first_known_distinct_agent_ids,
    distinct_agent_ids - max_session_id AS distinct_agent_ids_minus_max_session_id
FROM sequenced
WHERE distinct_agent_ids < 9
ORDER BY distinct_agent_ids_minus_max_session_id
