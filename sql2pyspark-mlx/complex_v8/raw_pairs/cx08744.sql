WITH recent_tickets AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        priority,
        channel,
        csat,
        ROUND(csat / NULLIF(requester_id, 0), 3) AS csat_per_requester_id,
        CASE WHEN priority IN ('P3', 'P2') THEN 'group_a' WHEN priority = 'P1' THEN 'group_b' ELSE 'other' END AS priority_group,
        COALESCE(priority, 'UNKNOWN') AS priority_or_unknown
    FROM tickets
    WHERE opened_at < TIMESTAMP '2025-03-02 09:00:00'
      AND channel IN ('chat', 'phone')
),
enriched AS (
    -- Join support_agents
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.opened_at,
        s.priority,
        s.channel,
        s.csat,
        s.csat_per_requester_id,
        s.priority_group,
        s.priority_or_unknown,
        su.team AS agent_team,
        su.agent_name,
        su.hired_on AS agent_hired_on
    FROM recent_tickets s
    LEFT JOIN support_agents su ON su.id = s.agent_id
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        FIRST_VALUE(opened_at) IGNORE NULLS OVER (PARTITION BY priority_or_unknown, channel ORDER BY requester_id DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_opened_at,
        FIRST_VALUE(agent_hired_on) IGNORE NULLS OVER (PARTITION BY priority_or_unknown, channel ORDER BY requester_id DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_agent_hired_on,
        FIRST_VALUE(requester_id) IGNORE NULLS OVER (PARTITION BY priority_or_unknown, channel ORDER BY requester_id DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_requester_id,
        ROUND(CUME_DIST() OVER (PARTITION BY priority_or_unknown, channel ORDER BY requester_id DESC NULLS FIRST), 3) AS requester_id_cume_dist,
        ROUND(AVG(requester_id) OVER w_csat, 2) AS csat_avg_requester_id,
        LAG(opened_at) OVER w_priority_or_unknown_channel_3 AS prev_opened_at
    FROM enriched
    WINDOW w_csat AS (PARTITION BY csat), w_priority_or_unknown_channel_3 AS (PARTITION BY priority_or_unknown, channel ORDER BY requester_id DESC NULLS FIRST, id)
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT opened_at, priority_or_unknown, csat_avg_requester_id
    FROM ranked
) src
PIVOT (
    MAX(csat_avg_requester_id)
    FOR priority_or_unknown IN ('P2', 'P1')
)
