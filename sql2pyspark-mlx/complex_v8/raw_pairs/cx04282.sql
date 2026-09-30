WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        DATEDIFF(DATE '2025-12-31', opened_at) AS days_since_opened_at
    FROM tickets
    WHERE MONTH(opened_at) IN (1, 2)
),
with_prev AS (
    -- Previous opened_at per agent_id
    SELECT *, LAG(opened_at) OVER (PARTITION BY agent_id ORDER BY opened_at, id) AS prev_opened_at
    FROM filtered
),
flagged AS (
    -- Flag rows that start a new session (gap > 2 hours)
    SELECT *, CASE WHEN prev_opened_at IS NULL OR (UNIX_TIMESTAMP(opened_at) - UNIX_TIMESTAMP(prev_opened_at)) / 60 > 120 THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY agent_id ORDER BY opened_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
annotated AS (
    -- Add calculated columns
    SELECT
        id,
        requester_id,
        opened_at,
        closed_at,
        priority,
        days_since_opened_at,
        prev_opened_at,
        new_session_flag,
        burst_id,
        COALESCE(burst_id, 0) AS burst_id_filled,
        FLOOR(requester_id / 5) * 5 AS requester_id_bucket,
        COALESCE(requester_id, 0) AS requester_id_filled
    FROM sessionized
)
SELECT
    id,
    requester_id,
    closed_at,
    priority,
    days_since_opened_at,
    prev_opened_at,
    new_session_flag,
    burst_id,
    burst_id_filled,
    requester_id_bucket,
    ROUND((UNIX_TIMESTAMP(closed_at) - UNIX_TIMESTAMP(prev_opened_at)) / 60, 1) AS minutes_prev_opened_at_to_closed_at,
    CONCAT(priority, '#', CAST(requester_id_bucket AS STRING)) AS priority_ref
FROM annotated
ORDER BY closed_at DESC, requester_id_bucket
