WITH recent AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        tags,
        CASE WHEN priority IN ('P3', 'P1') THEN 'group_a' WHEN priority = 'P4' THEN 'group_b' ELSE 'other' END AS priority_group
    FROM tickets
    WHERE agent_id >= 7
),
with_prev AS (
    -- Previous opened_at per priority_group
    SELECT *, LAG(opened_at) OVER (PARTITION BY priority_group ORDER BY opened_at, id) AS prev_opened_at
    FROM recent
),
flagged AS (
    -- Flag rows that start a new session (gap > 60 minutes)
    SELECT *, CASE WHEN prev_opened_at IS NULL OR opened_at > prev_opened_at + INTERVAL 60 MINUTES THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY priority_group ORDER BY opened_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
sessions AS (
    -- One row per session
    SELECT
        priority_group,
        session_no,
        MIN(opened_at) AS session_start,
        MAX(opened_at) AS session_end,
        COUNT(*) AS event_count,
        SUM(requester_id) AS session_requester_id,
        ROUND((UNIX_TIMESTAMP(MAX(opened_at)) - UNIX_TIMESTAMP(MIN(opened_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY priority_group, session_no
),
enriched AS (
    -- Add calculated columns
    SELECT
        priority_group,
        session_no,
        session_start,
        session_end,
        event_count,
        ABS(duration_min - 144.0) AS duration_min_distance,
        YEAR(session_start) AS session_start_year,
        CASE WHEN event_count >= 4 THEN 'large' WHEN event_count >= 3 THEN 'medium' ELSE 'small' END AS event_count_tier
    FROM sessions
    WHERE priority_group = 'group_b'
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        priority_group,
        session_no,
        session_start,
        session_end,
        event_count,
        duration_min_distance,
        DENSE_RANK() OVER w_all_by_event_count AS event_count_dense_rank,
        ROUND(PERCENT_RANK() OVER (PARTITION BY session_start_year, event_count_tier ORDER BY event_count), 3) AS event_count_percent_rank,
        SIZE(COLLECT_SET(priority_group) OVER (PARTITION BY session_start_year)) AS distinct_priority_group_in_session_start_year
    FROM enriched
    WINDOW w_all_by_event_count AS (ORDER BY event_count, priority_group, session_no)
)
-- Native SQL UNPIVOT (wide to long)
SELECT priority_group, session_no, metric_name, amount
FROM (
    SELECT priority_group, session_no, distinct_priority_group_in_session_start_year, event_count_dense_rank, event_count
    FROM metrics
) src
UNPIVOT (
    amount FOR metric_name IN (distinct_priority_group_in_session_start_year, event_count_dense_rank, event_count)
) unp
