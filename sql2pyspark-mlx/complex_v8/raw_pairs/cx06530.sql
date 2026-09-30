WITH filtered AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        priority,
        channel,
        csat,
        tags
    FROM tickets
),
with_prev AS (
    -- Previous closed_at per channel
    SELECT *, LAG(closed_at) OVER (PARTITION BY channel ORDER BY closed_at, id) AS prev_closed_at
    FROM filtered
),
flagged AS (
    -- Flag rows that start a new session (gap > 60 minutes)
    SELECT *, CASE WHEN prev_closed_at IS NULL OR (UNIX_TIMESTAMP(closed_at) - UNIX_TIMESTAMP(prev_closed_at)) / 60 > 60 THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY channel ORDER BY closed_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
summarized_rows AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        (SELECT MIN(t.requester_id) FROM tickets t WHERE t.agent_id = s.agent_id) AS min_ticket_requester_id
    FROM sessionized s
),
derived AS (
    -- Add calculated columns
    SELECT
        id,
        requester_id,
        closed_at,
        csat,
        tags,
        session_id,
        min_ticket_requester_id,
        CASE WHEN priority IN ('P4', 'P2') THEN 'group_a' WHEN priority = 'P3' THEN 'group_b' ELSE 'other' END AS priority_group,
        QUARTER(prev_closed_at) AS prev_closed_at_quarter,
        CASE WHEN channel IN ('email', 'phone') THEN 'group_a' WHEN channel = 'web' THEN 'group_b' ELSE 'other' END AS channel_group
    FROM summarized_rows
)
SELECT
    prev_closed_at_quarter,
    priority_group,
    session_id,
    GROUPING_ID() AS gid,
    ROUND(SUM(prev_closed_at_quarter) / NULLIF(SUM(csat), 0), 3) AS prev_closed_at_quarter_to_csat_ratio,
    COUNT(DISTINCT priority_group) AS distinct_priority_groups,
    SUM(requester_id) AS total_requester_id
FROM derived
GROUP BY GROUPING SETS ((prev_closed_at_quarter, priority_group, session_id), (prev_closed_at_quarter))
ORDER BY prev_closed_at_quarter NULLS LAST, priority_group NULLS LAST, session_id NULLS LAST
