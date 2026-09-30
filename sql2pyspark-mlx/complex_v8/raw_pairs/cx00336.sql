WITH cleaned_tickets AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        csat,
        tags,
        QUARTER(opened_at) AS opened_at_quarter
    FROM tickets
),
with_prev AS (
    -- Previous opened_at per agent_id
    SELECT *, LAG(opened_at) OVER (PARTITION BY agent_id ORDER BY opened_at, id) AS prev_opened_at
    FROM cleaned_tickets
),
flagged AS (
    -- Flag rows that start a new session (gap > 90 minutes)
    SELECT *, CASE WHEN prev_opened_at IS NULL OR opened_at > prev_opened_at + INTERVAL 90 MINUTES THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY agent_id ORDER BY opened_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
session_summary AS (
    -- One row per session
    SELECT
        agent_id,
        burst_id,
        MIN(opened_at) AS session_start,
        MAX(opened_at) AS session_end,
        COUNT(*) AS n_events,
        ROUND((UNIX_TIMESTAMP(MAX(opened_at)) - UNIX_TIMESTAMP(MIN(opened_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY agent_id, burst_id
),
ranked AS (
    -- Previous / next values per key
    SELECT
        agent_id,
        burst_id,
        session_start,
        session_end,
        n_events,
        MAX(duration_min) OVER (ORDER BY session_start ASC, agent_id, burst_id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS duration_min_max_3_prior,
        MIN(duration_min) OVER (PARTITION BY agent_id) AS agent_id_min_duration_min
    FROM session_summary
),
filtered AS (
    -- Compare against the overall max
    SELECT *
    FROM ranked
    WHERE burst_id < (SELECT MAX(burst_id) FROM ranked)
)
-- Native SQL UNPIVOT (wide to long)
SELECT agent_id, burst_id, metric_name, value
FROM (
    SELECT agent_id, burst_id, duration_min_max_3_prior, agent_id_min_duration_min
    FROM filtered
) src
UNPIVOT (
    value FOR metric_name IN (duration_min_max_3_prior, agent_id_min_duration_min)
) unp
