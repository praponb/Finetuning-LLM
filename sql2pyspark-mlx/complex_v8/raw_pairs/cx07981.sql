WITH scoped AS (
    -- Scope the tickets rows
    SELECT *
    FROM tickets
),
with_prev AS (
    -- Previous opened_at per agent_id
    SELECT *, LAG(opened_at) OVER (PARTITION BY agent_id ORDER BY opened_at, id) AS prev_opened_at
    FROM scoped
),
flagged AS (
    -- Flag rows that start a new session (gap > 30 minutes)
    SELECT *, CASE WHEN prev_opened_at IS NULL OR (UNIX_TIMESTAMP(opened_at) - UNIX_TIMESTAMP(prev_opened_at)) / 60 > 30 THEN 1 ELSE 0 END AS new_session_flag
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
        COUNT(*) AS events,
        SUM(requester_id) AS session_requester_id,
        ROUND((UNIX_TIMESTAMP(MAX(opened_at)) - UNIX_TIMESTAMP(MIN(opened_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY agent_id, burst_id
),
with_children AS (
    -- One row per ticket
    SELECT
        s.agent_id,
        s.burst_id,
        s.session_start,
        s.session_end,
        s.events,
        s.session_requester_id,
        s.duration_min,
        t.id AS ticket_id,
        t.tags AS ticket_tags,
        t.opened_at AS ticket_opened_at,
        t.closed_at AS ticket_closed_at,
        t.channel AS ticket_channel
    FROM session_summary s
    LEFT JOIN tickets t ON t.agent_id = s.agent_id AND t.channel <> 'email'
),
filtered AS (
    -- Rows with no matching tickets
    SELECT s.*
    FROM with_children s
    WHERE NOT EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.agent_id = s.agent_id
              AND t.channel = 'web'
        )
)
-- Subtotals with ROLLUP
SELECT
    ticket_id,
    agent_id,
    GROUPING(ticket_id) AS is_total_ticket_id,
    ROUND(SUM(duration_min), 2) AS total_duration_min,
    ROUND(SUM(duration_min) / NULLIF(SUM(burst_id), 0), 3) AS duration_min_to_burst_id_ratio
FROM filtered
GROUP BY ROLLUP(ticket_id, agent_id)
ORDER BY ticket_id NULLS LAST, agent_id NULLS LAST
