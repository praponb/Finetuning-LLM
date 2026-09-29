WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        csat,
        ROUND((UNIX_TIMESTAMP(opened_at) - UNIX_TIMESTAMP(closed_at)) / 3600, 1) AS hours_closed_at_to_opened_at,
        LENGTH(TRIM(priority)) AS priority_len,
        DATE_FORMAT(opened_at, 'MMM yyyy') AS opened_at_label
    FROM tickets
    WHERE priority = 'P3'
      AND channel <> 'web'
),
with_details AS (
    -- Join support_agents
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.csat,
        s.priority_len,
        s.opened_at_label,
        su.tier AS agent_tier,
        su.hired_on AS agent_hired_on
    FROM valid s
    LEFT JOIN support_agents su ON su.id = s.agent_id
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        MIN(priority_len) OVER w_agent_id AS agent_id_min_priority_len,
        SIZE(COLLECT_SET(agent_tier) OVER (PARTITION BY agent_id)) AS distinct_agent_tier_in_agent_id,
        SUM(csat) OVER (PARTITION BY priority_len, agent_id ORDER BY priority_len DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_csat
    FROM with_details
    WINDOW w_agent_id AS (PARTITION BY agent_id)
),
agent_tier_stats AS (
    -- Max priority_len per agent_tier
    SELECT agent_tier, MAX(priority_len) AS max_priority_len
    FROM with_windows
    GROUP BY agent_tier
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_priority_len,
        ROUND(s.priority_len - g.max_priority_len, 2) AS priority_len_minus_max
    FROM with_windows s
    JOIN agent_tier_stats g ON g.agent_tier = s.agent_tier
    WHERE s.priority_len = g.max_priority_len
)
SELECT
    agent_tier,
    priority_len,
    DATEDIFF(MAX(agent_hired_on), MIN(agent_hired_on)) AS agent_hired_on_span_days,
    ROUND(SUM(max_priority_len) / NULLIF(SUM(agent_id_min_priority_len), 0), 3) AS max_priority_len_to_agent_id_min_priority_len_ra
FROM outliers
GROUP BY CUBE(agent_tier, priority_len)
