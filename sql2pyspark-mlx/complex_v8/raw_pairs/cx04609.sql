WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        agent_id,
        opened_at,
        priority,
        channel,
        csat,
        tags
    FROM tickets
    WHERE csat > 3
),
with_parent AS (
    -- Attach agent attributes
    SELECT
        s.id,
        s.agent_id,
        s.priority,
        s.channel,
        s.csat,
        su.hired_on AS agent_hired_on
    FROM prepared s
    JOIN support_agents su ON su.id = s.agent_id
),
unique_rows AS (
    -- Keep the earliest row per priority, channel
    SELECT
        id,
        agent_id,
        priority,
        channel,
        csat,
        agent_hired_on
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY priority, channel ORDER BY agent_hired_on, id) AS recency_rank
        FROM with_parent
    ) ranked
    WHERE recency_rank = 1
),
rollup_base AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_FORMAT(agent_hired_on, 'yyyy-MM') AS agent_hired_on_ym,
        MIN(csat) AS min_csat,
        PERCENTILE_APPROX(csat, 0.5) AS median_csat,
        ROUND(AVG(csat), 2) AS avg_csat
    FROM unique_rows
    WHERE agent_id >= 7
    GROUP BY DATE_FORMAT(agent_hired_on, 'yyyy-MM')
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        agent_hired_on_ym,
        median_csat,
        avg_csat,
        ROUND(min_csat / NULLIF(SUM(min_csat) OVER (PARTITION BY median_csat), 0), 4) AS min_csat_share_of_median_csat,
        DENSE_RANK() OVER (ORDER BY median_csat ASC, agent_hired_on_ym) AS median_csat_dense_rank
    FROM rollup_base
),
tagged AS (
    -- Add calculated columns
    SELECT
        agent_hired_on_ym,
        avg_csat,
        min_csat_share_of_median_csat,
        median_csat_dense_rank,
        avg_csat + median_csat_dense_rank AS avg_csat_plus_median_csat_dense_rank,
        ROUND(avg_csat * 0.9, 2) AS avg_csat_adj
    FROM sequenced
)
-- Final projection
SELECT
    agent_hired_on_ym,
    avg_csat,
    median_csat_dense_rank,
    avg_csat_plus_median_csat_dense_rank,
    avg_csat_adj,
    ROUND(avg_csat_adj * 1.5, 2) AS avg_csat_adj_adj,
    ROUND(avg_csat_adj * 0.9, 2) AS avg_csat_adj_adj_2
FROM tagged
ORDER BY avg_csat, avg_csat_adj_adj_2 DESC
