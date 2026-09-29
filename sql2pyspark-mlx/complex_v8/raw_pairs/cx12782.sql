WITH valid_tickets AS (
    -- Scope the tickets rows
    SELECT
        id,
        agent_id,
        opened_at,
        channel,
        csat
    FROM tickets
),
with_prev AS (
    -- Previous opened_at per agent_id
    SELECT *, LAG(opened_at) OVER (PARTITION BY agent_id ORDER BY opened_at, id) AS prev_opened_at
    FROM valid_tickets
),
flagged AS (
    -- Flag rows that start a new session (gap > 90 minutes)
    SELECT *, CASE WHEN prev_opened_at IS NULL OR opened_at > prev_opened_at + INTERVAL 90 MINUTES THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY agent_id ORDER BY opened_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
joined AS (
    -- Attach agent attributes
    SELECT s.*, su.team AS agent_team
    FROM sessionized s
    JOIN support_agents su ON su.id = s.agent_id
)
SELECT
    id,
    agent_id,
    opened_at,
    csat,
    prev_opened_at,
    new_session_flag,
    session_id,
    agent_team
FROM joined
WHERE csat >= 1
ORDER BY prev_opened_at, agent_id DESC
