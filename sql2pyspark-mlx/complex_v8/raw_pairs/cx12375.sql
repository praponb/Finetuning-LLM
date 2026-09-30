WITH filtered AS (
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
        ROUND((UNIX_TIMESTAMP(closed_at) - UNIX_TIMESTAMP(opened_at)) / 3600, 1) AS hours_opened_at_to_closed_at
    FROM tickets
    WHERE ARRAY_CONTAINS(tags, 'bug')
),
with_prev AS (
    -- Previous opened_at per agent_id
    SELECT *, LAG(opened_at) OVER (PARTITION BY agent_id ORDER BY opened_at, id) AS prev_opened_at
    FROM filtered
),
flagged AS (
    -- Flag rows that start a new session (gap > 30 minutes)
    SELECT *, CASE WHEN prev_opened_at IS NULL OR opened_at > prev_opened_at + INTERVAL 30 MINUTES THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY agent_id ORDER BY opened_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
profiled AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM ticket_events t WHERE t.ticket_id = s.id) AS event_count,
        (SELECT MIN(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.id) AS first_event_at,
        (SELECT MAX(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.id) AS last_event_at
    FROM sessionized s
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SUM(hours_opened_at_to_closed_at) OVER (PARTITION BY TO_DATE(prev_opened_at), event_count) AS prev_opened_at_to_date_event_count_sum_hours_ope,
        ROUND(requester_id - AVG(requester_id) OVER (PARTITION BY TO_DATE(prev_opened_at), event_count), 2) AS requester_id_vs_prev_opened_at_to_date_event_cou,
        LAG(burst_id) OVER (PARTITION BY new_session_flag, agent_id ORDER BY burst_id DESC, id) AS prev_burst_id,
        SUM(burst_id) OVER (PARTITION BY new_session_flag, agent_id ORDER BY burst_id DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_burst_id
    FROM profiled
)
SELECT
    agent_id,
    channel,
    priority,
    GROUPING(agent_id) AS is_total_agent_id,
    ROUND(STDDEV(prev_opened_at_to_date_event_count_sum_hours_ope), 2) AS stddev_prev_opened_at_to_date_event_count_sum_ho,
    MAX(burst_id) AS max_burst_id,
    ROUND(AVG(running_burst_id), 2) AS avg_running_burst_id
FROM metrics
GROUP BY GROUPING SETS ((agent_id, channel, priority), (agent_id), ())
ORDER BY agent_id NULLS LAST, channel NULLS LAST, priority NULLS LAST
