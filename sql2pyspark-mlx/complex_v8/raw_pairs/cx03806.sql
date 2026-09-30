WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        priority,
        channel,
        csat,
        tags,
        DATE_TRUNC('HOUR', opened_at) AS opened_at_hour,
        FLOOR(requester_id / 5) * 5 AS requester_id_bucket
    FROM tickets
),
deduped AS (
    -- Keep the latest row per agent_id, channel
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        priority,
        channel,
        csat,
        tags,
        opened_at_hour,
        requester_id_bucket
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY agent_id, channel ORDER BY csat DESC, id DESC) AS rn
        FROM valid
    ) ranked
    WHERE rn = 1
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(csat / NULLIF(SUM(csat) OVER (PARTITION BY agent_id), 0), 4) AS csat_share_of_agent_id,
        COUNT(*) OVER w_agent_id AS rows_in_agent_id,
        MIN(requester_id_bucket) OVER (PARTITION BY agent_id) AS agent_id_min_requester_id_bucket,
        NTH_VALUE(requester_id_bucket, 2) OVER (ORDER BY closed_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_requester_id_bucket,
        SUM(requester_id_bucket) OVER (PARTITION BY agent_id) AS agent_id_sum_requester_id_bucket,
        ROUND(csat - AVG(csat) OVER w_agent_id, 2) AS csat_vs_agent_id_avg
    FROM deduped
    WINDOW w_agent_id AS (PARTITION BY agent_id)
)
SELECT
    agent_id,
    csat,
    priority,
    ROUND(SUM(second_requester_id_bucket) / NULLIF(SUM(agent_id_min_requester_id_bucket), 0), 3) AS second_requester_id_bucket_to_agent_id_min_reque,
    MAX(agent_id_sum_requester_id_bucket) AS max_agent_id_sum_requester_id_bucket,
    COUNT(*) AS n_rows
FROM ranked
GROUP BY CUBE(agent_id, csat, priority)
ORDER BY agent_id NULLS LAST, csat NULLS LAST, priority NULLS LAST
