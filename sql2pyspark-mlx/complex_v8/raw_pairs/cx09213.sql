WITH filtered_tickets AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        priority,
        channel,
        tags,
        CASE WHEN channel IN ('phone', 'web') THEN 'group_a' WHEN channel = 'chat' THEN 'group_b' ELSE 'other' END AS channel_group
    FROM tickets
    WHERE NOT ARRAY_CONTAINS(tags, 'upgrade')
),
with_prev AS (
    -- Previous closed_at per priority
    SELECT *, LAG(closed_at) OVER (PARTITION BY priority ORDER BY closed_at, id) AS prev_closed_at
    FROM filtered_tickets
),
flagged AS (
    -- Flag rows that start a new session (gap > 60 minutes)
    SELECT *, CASE WHEN prev_closed_at IS NULL OR closed_at > prev_closed_at + INTERVAL 60 MINUTES THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY priority ORDER BY closed_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        id,
        requester_id,
        closed_at,
        channel,
        new_session_flag,
        session_id,
        SIZE(COLLECT_SET(priority) OVER (PARTITION BY session_id)) AS distinct_priority_in_session_id,
        RANK() OVER (PARTITION BY new_session_flag, MONTH(closed_at) ORDER BY closed_at ASC) AS closed_at_rank,
        ROUND(new_session_flag / NULLIF(SUM(new_session_flag) OVER w_session_id, 0), 4) AS new_session_flag_share_of_session_id,
        NTH_VALUE(prev_closed_at, 2) OVER (PARTITION BY new_session_flag, MONTH(closed_at) ORDER BY closed_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_prev_closed_at
    FROM sessionized
    WINDOW w_session_id AS (PARTITION BY session_id)
)
SELECT
    id,
    requester_id,
    closed_at,
    new_session_flag,
    distinct_priority_in_session_id,
    closed_at_rank,
    new_session_flag_share_of_session_id,
    second_prev_closed_at,
    CASE WHEN channel IN ('phone', 'email') THEN 'group_a' WHEN channel = 'chat' THEN 'group_b' ELSE 'other' END AS channel_group
FROM with_windows
ORDER BY closed_at_rank
