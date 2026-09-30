WITH recent AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        priority,
        tags,
        ROUND(requester_id / NULLIF(csat, 0), 3) AS requester_id_per_csat
    FROM tickets
),
with_prev AS (
    -- Previous closed_at per priority
    SELECT *, LAG(closed_at) OVER (PARTITION BY priority ORDER BY closed_at, id) AS prev_closed_at
    FROM recent
),
flagged AS (
    -- Flag rows that start a new session (gap > 60 minutes)
    SELECT *, CASE WHEN prev_closed_at IS NULL OR (UNIX_TIMESTAMP(closed_at) - UNIX_TIMESTAMP(prev_closed_at)) / 60 > 60 THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY priority ORDER BY closed_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
per_session AS (
    -- One row per session
    SELECT
        priority,
        session_no,
        MIN(closed_at) AS session_start,
        MAX(closed_at) AS session_end,
        COUNT(*) AS event_count,
        ROUND((UNIX_TIMESTAMP(MAX(closed_at)) - UNIX_TIMESTAMP(MIN(closed_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY priority, session_no
),
scored AS (
    -- Add calculated columns
    SELECT
        *,
        CASE WHEN priority IN ('P1', 'P4') THEN 'group_a' WHEN priority = 'P2' THEN 'group_b' ELSE 'other' END AS priority_group,
        LOWER(REGEXP_REPLACE(TRIM(priority), ' ', '_')) AS priority_slug,
        CONCAT(priority, '#', CAST(event_count AS STRING)) AS priority_ref
    FROM per_session
    WHERE priority NOT IN ('P4', 'P2')
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        priority,
        session_no,
        session_end,
        event_count,
        priority_group,
        priority_slug,
        priority_ref,
        ROUND(AVG(event_count) OVER (PARTITION BY YEAR(session_end) ORDER BY session_start, priority, session_no ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS event_count_avg_4,
        MIN(event_count) OVER w_session_start_to_date AS session_start_to_date_min_event_count
    FROM scored
    WINDOW w_session_start_to_date AS (PARTITION BY TO_DATE(session_start))
)
SELECT *
FROM (
    SELECT session_end, session_no, priority_group, event_count
    FROM windowed
) src
PIVOT (
    MAX(event_count)
    FOR priority_group IN ('group_b', 'other', 'group_a')
)
ORDER BY session_end, session_no
