WITH filtered_tickets AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        priority,
        channel,
        csat
    FROM tickets
),
with_prev AS (
    -- Previous closed_at per agent_id
    SELECT *, LAG(closed_at) OVER (PARTITION BY agent_id ORDER BY closed_at, id) AS prev_closed_at
    FROM filtered_tickets
),
flagged AS (
    -- Flag rows that start a new session (gap > 45 minutes)
    SELECT *, CASE WHEN prev_closed_at IS NULL OR (UNIX_TIMESTAMP(closed_at) - UNIX_TIMESTAMP(prev_closed_at)) / 60 > 45 THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY agent_id ORDER BY closed_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        priority,
        channel,
        csat,
        prev_closed_at,
        starts_session,
        session_id,
        FIRST_VALUE(csat) OVER (ORDER BY closed_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_csat,
        starts_session - LAG(starts_session) OVER (ORDER BY closed_at DESC, id) AS starts_session_change,
        NTH_VALUE(csat, 2) OVER (ORDER BY closed_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_csat,
        DENSE_RANK() OVER w_starts_session_by_prev_close AS prev_closed_at_dense_rank,
        NTILE(10) OVER (ORDER BY closed_at DESC, id) AS closed_at_decile,
        ROUND(PERCENT_RANK() OVER (ORDER BY closed_at DESC, id), 3) AS closed_at_percent_rank
    FROM sessionized
    WINDOW w_starts_session_by_prev_close AS (PARTITION BY starts_session ORDER BY prev_closed_at DESC)
),
priority_stats AS (
    -- Avg requester_id per priority
    SELECT priority, AVG(requester_id) AS avg_requester_id
    FROM metrics
    GROUP BY priority
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_requester_id,
        ROUND(s.requester_id - g.avg_requester_id, 2) AS requester_id_minus_avg
    FROM metrics s
    JOIN priority_stats g ON g.priority = s.priority
    WHERE s.requester_id > g.avg_requester_id
)
SELECT id, measure, metric_value
FROM (
    SELECT id, avg_requester_id, requester_id_minus_avg, closed_at_percent_rank
    FROM compared
) src
UNPIVOT (
    metric_value FOR measure IN (avg_requester_id AS avg_requester_id_v, requester_id_minus_avg AS requester_id_minus_avg_v, closed_at_percent_rank AS m_closed_at_percent_rank)
) unp
