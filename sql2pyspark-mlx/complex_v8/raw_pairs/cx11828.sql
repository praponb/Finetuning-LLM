WITH scoped_tickets AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        channel,
        csat,
        tags
    FROM tickets
),
with_prev AS (
    -- Previous opened_at per channel
    SELECT *, LAG(opened_at) OVER (PARTITION BY channel ORDER BY opened_at, id) AS prev_opened_at
    FROM scoped_tickets
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
with_activity AS (
    -- Rows that have matching tickets
    SELECT s.*
    FROM sessionized s
    WHERE s.agent_id IN (
            SELECT t.agent_id
            FROM tickets t
            WHERE t.closed_at >= TIMESTAMP '2025-02-08 15:00:00'
              AND NOT ARRAY_CONTAINS(t.tags, 'refund')
        )
),
labeled AS (
    -- Derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        channel,
        tags,
        prev_opened_at,
        new_session_flag,
        session_no,
        ROUND(new_session_flag / NULLIF(requester_id, 0), 3) AS new_session_flag_per_requester_id,
        DATE_TRUNC('WEEK', prev_opened_at) AS prev_opened_at_week
    FROM with_activity
)
SELECT
    id,
    requester_id,
    agent_id,
    opened_at,
    channel,
    new_session_flag,
    session_no,
    prev_opened_at_week,
    ARRAY_CONTAINS(tags, 'outage') AS has_outage
FROM labeled
