WITH cleaned AS (
    -- Filter tickets
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        csat,
        tags,
        SUBSTRING(TRIM(priority), 1, 4) AS priority_prefix
    FROM tickets
    WHERE csat <= 4
      AND ARRAY_CONTAINS(tags, 'bug')
),
with_details AS (
    -- Join support_agents
    SELECT s.*, su.hired_on AS agent_hired_on
    FROM cleaned s
    JOIN support_agents su ON su.id = s.agent_id
    WHERE su.hired_on < DATE '2025-02-26'
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        csat,
        tags,
        agent_hired_on,
        ROUND(csat / NULLIF(SUM(csat) OVER w_closed_at_year, 0), 4) AS csat_share_of_closed_at_year,
        NTILE(4) OVER (PARTITION BY channel ORDER BY agent_hired_on NULLS LAST, id) AS agent_hired_on_quartile,
        ROUND(csat / NULLIF(SUM(csat) OVER (PARTITION BY csat, priority), 0), 4) AS csat_share_of_csat_priority,
        LAG(agent_hired_on) OVER w_agent_id_by_csat AS prev_agent_hired_on,
        NTH_VALUE(agent_hired_on, 2) OVER (PARTITION BY channel ORDER BY agent_hired_on NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_agent_hired_on,
        LAG(agent_hired_on) OVER w_csat_priority_by_opened_at AS prev_agent_hired_on_2,
        MAX(csat) OVER (PARTITION BY csat, priority ORDER BY opened_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_csat_so_far,
        MIN(csat) OVER (PARTITION BY agent_id ORDER BY csat, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_csat_so_far,
        ROUND(AVG(csat) OVER (PARTITION BY csat, priority), 2) AS csat_priority_avg_csat,
        MIN(csat) OVER (PARTITION BY YEAR(closed_at)) AS closed_at_year_min_csat,
        ROUND(csat - AVG(csat) OVER (PARTITION BY YEAR(closed_at)), 2) AS csat_vs_closed_at_year_avg,
        DENSE_RANK() OVER (PARTITION BY agent_id ORDER BY csat) AS csat_dense_rank
    FROM with_details
    WINDOW w_closed_at_year AS (PARTITION BY YEAR(closed_at)), w_agent_id_by_csat AS (PARTITION BY agent_id ORDER BY csat, id), w_csat_priority_by_opened_at AS (PARTITION BY csat, priority ORDER BY opened_at DESC, id)
),
top_rows AS (
    -- First row per group
    SELECT *
    FROM with_trends
    WHERE csat_dense_rank = 1
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        closed_at,
        priority,
        agent_hired_on,
        csat_share_of_closed_at_year,
        agent_hired_on_quartile,
        csat_share_of_csat_priority,
        second_agent_hired_on,
        prev_agent_hired_on_2,
        max_csat_so_far,
        min_csat_so_far,
        closed_at_year_min_csat,
        csat_vs_closed_at_year_avg,
        MAX(csat_vs_closed_at_year_avg) OVER (PARTITION BY priority) AS priority_max_csat_vs_closed_at_year_avg,
        ROUND(agent_hired_on_quartile - AVG(agent_hired_on_quartile) OVER (PARTITION BY priority), 2) AS agent_hired_on_quartile_vs_priority_avg,
        DENSE_RANK() OVER (ORDER BY csat_share_of_closed_at_year DESC) AS csat_share_of_closed_at_year_dense_rank,
        DENSE_RANK() OVER (ORDER BY csat_share_of_closed_at_year DESC, id) AS csat_share_of_closed_at_year_dense_rank_2
    FROM top_rows
)
-- Final projection
SELECT
    id,
    agent_hired_on,
    csat_share_of_closed_at_year,
    agent_hired_on_quartile,
    second_agent_hired_on,
    prev_agent_hired_on_2,
    max_csat_so_far,
    min_csat_so_far,
    csat_vs_closed_at_year_avg,
    priority_max_csat_vs_closed_at_year_avg,
    csat_share_of_closed_at_year_dense_rank,
    csat_share_of_closed_at_year_dense_rank_2,
    DATEDIFF(DATE '2025-06-30', second_agent_hired_on) AS days_since_second_agent_hired_on
FROM windowed
ORDER BY csat_share_of_closed_at_year_dense_rank NULLS LAST, agent_hired_on_quartile DESC
