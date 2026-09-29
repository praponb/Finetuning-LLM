WITH prepared_tickets AS (
    -- Filter tickets
    SELECT *
    FROM tickets
    WHERE opened_at >= TIMESTAMP '2025-01-07 13:00:00'
),
with_prev AS (
    -- Previous opened_at per agent_id
    SELECT *, LAG(opened_at) OVER (PARTITION BY agent_id ORDER BY opened_at, id) AS prev_opened_at
    FROM prepared_tickets
),
flagged AS (
    -- Flag rows that start a new session (gap > 90 minutes)
    SELECT *, CASE WHEN prev_opened_at IS NULL OR (UNIX_TIMESTAMP(opened_at) - UNIX_TIMESTAMP(prev_opened_at)) / 60 > 90 THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY agent_id ORDER BY opened_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
)
SELECT
    is_new_session,
    csat,
    agent_id,
    GROUPING(is_new_session) AS is_total_is_new_session,
    COUNT(DISTINCT priority) AS distinct_prioritys
FROM sessionized
GROUP BY CUBE(is_new_session, csat, agent_id)
ORDER BY is_new_session NULLS LAST, csat NULLS LAST, agent_id NULLS LAST
