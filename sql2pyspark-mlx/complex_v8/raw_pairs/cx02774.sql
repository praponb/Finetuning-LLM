WITH recent_tickets AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        csat
    FROM tickets
    WHERE csat BETWEEN 2 AND 4
),
with_prev AS (
    -- Previous opened_at per channel
    SELECT *, LAG(opened_at) OVER (PARTITION BY channel ORDER BY opened_at, id) AS prev_opened_at
    FROM recent_tickets
),
flagged AS (
    -- Flag rows that start a new session (gap > 90 minutes)
    SELECT *, CASE WHEN prev_opened_at IS NULL OR opened_at > prev_opened_at + INTERVAL 90 MINUTES THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY channel ORDER BY opened_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
screened AS (
    -- Rows with no matching ticket_events
    SELECT s.*
    FROM sessionized s
    WHERE NOT EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.id
              AND HOUR(t.event_at) BETWEEN 8 AND 10
              AND t.event_type = 'resolved'
        )
      AND s.csat BETWEEN 1 AND 5
),
joined_agent AS (
    -- Enrich with dimension data
    SELECT s.*, su.hired_on AS agent_hired_on, su.tier AS agent_tier
    FROM screened s
    JOIN support_agents su ON su.id = s.agent_id AND su.hired_on BETWEEN DATE '2024-05-02' AND DATE '2025-11-09'
)
SELECT
    id,
    agent_id,
    priority,
    channel,
    prev_opened_at,
    new_session_flag,
    agent_hired_on,
    agent_tier
FROM joined_agent
ORDER BY priority DESC
