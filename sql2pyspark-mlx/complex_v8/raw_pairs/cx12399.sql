WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        agent_id,
        opened_at,
        priority,
        csat,
        tags
    FROM tickets
    WHERE TO_DATE(opened_at) < DATE '2025-02-12'
      AND requester_id >= 8
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        priority,
        csat,
        LAST_VALUE(opened_at) IGNORE NULLS OVER (PARTITION BY agent_id ORDER BY csat ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_opened_at,
        RANK() OVER (PARTITION BY agent_id ORDER BY csat ASC, id) AS csat_rank,
        ROUND(csat - AVG(csat) OVER (PARTITION BY csat, agent_id), 2) AS csat_vs_csat_agent_id_avg
    FROM recent
),
latest AS (
    -- Keep the latest row per priority
    SELECT
        id,
        priority,
        csat,
        last_known_opened_at,
        csat_rank,
        csat_vs_csat_agent_id_avg
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY priority ORDER BY last_known_opened_at DESC, id DESC) AS recency_rank
        FROM sequenced
    ) ranked
    WHERE recency_rank = 1
)
SELECT
    id,
    priority,
    csat,
    csat_rank,
    csat_vs_csat_agent_id_avg,
    DATE_FORMAT(last_known_opened_at, 'yyyy-MM-dd HH:00') AS last_known_opened_at_label,
    ABS(csat_rank - 7) AS csat_rank_distance
FROM latest
ORDER BY csat_rank DESC
