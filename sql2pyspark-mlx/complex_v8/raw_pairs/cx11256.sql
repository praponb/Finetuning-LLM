WITH scoped AS (
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
        CASE WHEN priority IN ('P1', 'P2') THEN 'group_a' WHEN priority = 'P4' THEN 'group_b' ELSE 'other' END AS priority_group
    FROM tickets
),
with_prev AS (
    -- Previous opened_at per channel
    SELECT *, LAG(opened_at) OVER (PARTITION BY channel ORDER BY opened_at, id) AS prev_opened_at
    FROM scoped
),
flagged AS (
    -- Flag rows that start a new session (gap > 2 hours)
    SELECT *, CASE WHEN prev_opened_at IS NULL OR opened_at > prev_opened_at + INTERVAL 2 HOURS THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY channel ORDER BY opened_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
enriched_agent AS (
    -- Join support_agents
    SELECT
        s.id,
        s.agent_id,
        s.opened_at,
        s.closed_at,
        s.priority,
        s.channel,
        s.tags,
        s.priority_group,
        s.prev_opened_at,
        s.session_id,
        su.hired_on AS agent_hired_on
    FROM sessionized s
    LEFT JOIN support_agents su ON su.id = s.agent_id AND su.hired_on >= DATE '2024-05-02'
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        opened_at,
        tags,
        prev_opened_at,
        session_id,
        ROUND(session_id / NULLIF(SUM(session_id) OVER w_closed_at_to_date, 0), 4) AS session_id_share_of_closed_at_to_date,
        ROUND(CUME_DIST() OVER (PARTITION BY agent_id, session_id ORDER BY opened_at ASC), 3) AS opened_at_cume_dist,
        SIZE(COLLECT_SET(channel) OVER w_closed_at_to_date) AS distinct_channel_in_closed_at_to_date
    FROM enriched_agent
    WINDOW w_closed_at_to_date AS (PARTITION BY TO_DATE(closed_at))
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, metric_name, amount
FROM (
    SELECT id, session_id_share_of_closed_at_to_date, opened_at_cume_dist
    FROM sequenced
) src
UNPIVOT (
    amount FOR metric_name IN (session_id_share_of_closed_at_to_date, opened_at_cume_dist)
) unp
