WITH prepared AS (
    -- Scope the tickets rows
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        csat,
        tags,
        DATE_TRUNC('MONTH', opened_at) AS opened_at_month
    FROM tickets
    WHERE channel NOT IN ('phone', 'chat')
),
latest_per_key AS (
    -- Keep the latest row per agent_id
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        csat,
        tags,
        opened_at_month
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY agent_id ORDER BY opened_at DESC, id DESC) AS row_num
        FROM prepared
    ) ranked
    WHERE row_num = 1
),
with_details_agent AS (
    -- Attach agent attributes
    SELECT
        s.id,
        s.agent_id,
        s.opened_at,
        s.closed_at,
        s.priority,
        s.csat,
        s.tags,
        s.opened_at_month,
        su.agent_name,
        su.hired_on AS agent_hired_on
    FROM latest_per_key s
    JOIN support_agents su ON su.id = s.agent_id
    WHERE su.hired_on BETWEEN DATE '2024-05-02' AND DATE '2025-03-30'
),
annotated AS (
    -- Derive helper columns
    SELECT *, CONCAT(agent_name, '#', CAST(csat AS STRING)) AS agent_name_ref
    FROM with_details_agent
),
agent_id_stats AS (
    -- Avg csat per agent_id
    SELECT agent_id, AVG(csat) AS avg_csat
    FROM annotated
    GROUP BY agent_id
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.avg_csat, ROUND(s.csat - g.avg_csat, 2) AS csat_minus_avg
    FROM annotated s
    JOIN agent_id_stats g ON g.agent_id = s.agent_id
    WHERE s.csat > g.avg_csat
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT agent_hired_on, priority, csat_minus_avg
    FROM outliers
) src
PIVOT (
    SUM(csat_minus_avg) AS amount, MAX(csat_minus_avg) AS max_v
    FOR priority IN ('P4', 'P2')
)
ORDER BY agent_hired_on
