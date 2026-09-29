WITH prepared AS (
    -- Scope the tickets rows
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        channel,
        csat,
        tags,
        DATE_TRUNC('YEAR', opened_at) AS opened_at_year,
        CASE WHEN requester_id >= 20 THEN 'HIGH' WHEN requester_id >= 8 THEN 'MEDIUM' ELSE 'LOW' END AS requester_id_tier
    FROM tickets
    WHERE closed_at < TIMESTAMP '2025-02-19 21:00:00'
),
agent_id_stats AS (
    -- Median csat per agent_id
    SELECT agent_id, PERCENTILE_APPROX(csat, 0.5) AS median_csat
    FROM prepared
    GROUP BY agent_id
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.median_csat, ROUND(s.csat - g.median_csat, 2) AS csat_minus_median
    FROM prepared s
    JOIN agent_id_stats g ON g.agent_id = s.agent_id
    WHERE s.csat >= g.median_csat
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        RANK() OVER (PARTITION BY agent_id, csat ORDER BY median_csat DESC) AS median_csat_rank,
        MIN(csat_minus_median) OVER w_requester_id_tier_agent_id AS requester_id_tier_agent_id_min_csat_minus_median,
        FIRST_VALUE(opened_at) OVER (PARTITION BY agent_id, csat ORDER BY median_csat DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_opened_at,
        NTH_VALUE(opened_at, 2) OVER (PARTITION BY agent_id, csat ORDER BY median_csat DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_opened_at,
        ROUND(csat - AVG(csat) OVER (PARTITION BY requester_id_tier, agent_id), 2) AS csat_vs_requester_id_tier_agent_id_avg
    FROM outliers
    WINDOW w_requester_id_tier_agent_id AS (PARTITION BY requester_id_tier, agent_id)
)
SELECT
    requester_id_tier,
    agent_id,
    csat,
    GROUPING(requester_id_tier) AS is_total_requester_id_tier,
    MIN(median_csat_rank) AS min_median_csat_rank
FROM metrics
GROUP BY GROUPING SETS ((requester_id_tier, agent_id, csat), (requester_id_tier), ())
