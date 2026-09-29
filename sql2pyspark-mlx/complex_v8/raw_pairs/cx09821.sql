WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        csat,
        tags
    FROM tickets
    WHERE requester_id BETWEEN 2 AND 25
),
without_matches AS (
    -- Rows that have matching tickets
    SELECT s.*
    FROM prepared s
    WHERE EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.agent_id = s.agent_id
        )
),
agent_id_stats AS (
    -- Avg csat per agent_id
    SELECT agent_id, AVG(csat) AS avg_csat
    FROM without_matches
    GROUP BY agent_id
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.avg_csat, ROUND(s.csat - g.avg_csat, 2) AS csat_minus_avg
    FROM without_matches s
    JOIN agent_id_stats g ON g.agent_id = s.agent_id
    WHERE s.csat > g.avg_csat
),
with_parent AS (
    -- Attach agent attributes
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.opened_at,
        s.csat,
        s.csat_minus_avg,
        su.tier AS agent_tier,
        su.hired_on AS agent_hired_on
    FROM outliers s
    JOIN support_agents su ON su.id = s.agent_id AND UPPER(TRIM(su.agent_name)) LIKE 'L%'
    WHERE su.hired_on < DATE '2024-12-31'
),
with_activity AS (
    -- Keep rows whose agent matches
    SELECT s.*
    FROM with_parent s
    WHERE NOT EXISTS (
            SELECT 1
            FROM support_agents su
            WHERE su.id = s.agent_id
              AND su.hired_on BETWEEN DATE '2024-05-02' AND DATE '2025-02-27'
        )
),
with_details_agent AS (
    -- Attach agent attributes
    SELECT
        s.id,
        s.agent_id,
        s.opened_at,
        s.csat,
        s.agent_tier,
        s.agent_hired_on,
        su.tier AS agent_tier_2
    FROM with_activity s
    JOIN support_agents su ON su.id = s.agent_id
),
agent_id_stats_2 AS (
    -- Median csat per agent_id
    SELECT agent_id, PERCENTILE_APPROX(csat, 0.5) AS median_csat
    FROM with_details_agent
    GROUP BY agent_id
),
compared AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.median_csat, ROUND(s.csat - g.median_csat, 2) AS csat_minus_median
    FROM with_details_agent s
    JOIN agent_id_stats_2 g ON g.agent_id = s.agent_id
    WHERE s.csat >= g.median_csat
)
-- Final projection
SELECT
    id,
    csat,
    agent_tier,
    median_csat,
    ABS(csat_minus_median - -2.0) AS csat_minus_median_distance,
    TRUNC(agent_hired_on, 'MM') AS agent_hired_on_month_start
FROM compared
