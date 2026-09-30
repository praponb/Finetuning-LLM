WITH cleaned AS (
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
        ROUND((UNIX_TIMESTAMP(closed_at) - UNIX_TIMESTAMP(opened_at)) / 60, 1) AS minutes_opened_at_to_closed_at,
        LENGTH(TRIM(priority)) AS priority_len
    FROM tickets
    WHERE HOUR(closed_at) BETWEEN 10 AND 15
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        priority,
        channel,
        csat,
        tags,
        priority_len,
        COUNT(*) OVER w_opened_at_to_date AS rows_in_opened_at_to_date,
        SIZE(COLLECT_SET(channel) OVER w_opened_at_to_date) AS distinct_channel_in_opened_at_to_date,
        ROUND((UNIX_TIMESTAMP(closed_at) - UNIX_TIMESTAMP(LAG(closed_at) OVER w_all_by_priority_len)) / 60, 1) AS minutes_since_prev_closed_at,
        LAG(csat, 2) OVER (ORDER BY priority_len, id) AS prev2_csat,
        csat - LAG(csat) OVER (ORDER BY priority_len, id) AS csat_change
    FROM cleaned
    WINDOW w_opened_at_to_date AS (PARTITION BY TO_DATE(opened_at)), w_all_by_priority_len AS (ORDER BY priority_len, id)
),
without_matches AS (
    -- Rows that have matching ticket_events
    SELECT s.*
    FROM windowed s
    WHERE EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.id
        )
),
ticket_stats AS (
    -- Per-agent tickets metrics
    SELECT
        agent_id,
        COUNT(*) FILTER (WHERE priority = 'P1') AS ticket_priority_count
    FROM tickets
    GROUP BY agent_id
),
profiled AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        COALESCE(t.ticket_priority_count, 0) AS ticket_priority_count
    FROM without_matches s
    LEFT JOIN ticket_stats t ON t.agent_id = s.agent_id
)
-- Subtotals with GROUPING SETS
SELECT
    priority,
    priority_len,
    COUNT(*) AS cnt,
    SUM(CASE WHEN priority IN ('P1', 'P3') THEN csat_change ELSE 0 END) AS csat_change_priority_in_p1_p3
FROM profiled
GROUP BY GROUPING SETS ((priority, priority_len), (priority))
