WITH cleaned_tickets AS (
    -- Scope the tickets rows
    SELECT
        id,
        agent_id,
        closed_at,
        priority,
        csat,
        HOUR(opened_at) AS opened_at_hour
    FROM tickets
    WHERE channel IS NOT NULL
),
with_prev AS (
    -- Previous closed_at per priority
    SELECT *, LAG(closed_at) OVER (PARTITION BY priority ORDER BY closed_at, id) AS prev_closed_at
    FROM cleaned_tickets
),
flagged AS (
    -- Flag rows that start a new session (gap > 30 minutes)
    SELECT *, CASE WHEN prev_closed_at IS NULL OR closed_at > prev_closed_at + INTERVAL 30 MINUTES THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY priority ORDER BY closed_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
filtered AS (
    -- Compare against the overall avg
    SELECT *
    FROM sessionized
    WHERE session_no > (SELECT AVG(session_no) FROM sessionized)
),
labeled AS (
    -- Derive helper columns
    SELECT *, COALESCE(priority, 'UNKNOWN') AS priority_or_unknown
    FROM filtered
    WHERE MONTH(closed_at) IN (1, 3)
)
-- Final projection
SELECT
    id,
    priority,
    csat,
    opened_at_hour,
    prev_closed_at,
    starts_session,
    SUBSTRING(TRIM(priority_or_unknown), 1, 2) AS priority_or_unknown_prefix
FROM labeled
ORDER BY opened_at_hour
