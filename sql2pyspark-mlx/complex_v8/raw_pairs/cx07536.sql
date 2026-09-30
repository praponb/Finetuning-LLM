WITH valid_tickets AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        csat,
        tags,
        csat + requester_id AS csat_plus_requester_id
    FROM tickets
    WHERE TO_DATE(closed_at) < DATE '2025-02-10'
      AND agent_id >= 9
),
deduped AS (
    -- Keep the latest row per agent_id
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        csat,
        tags,
        csat_plus_requester_id
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY agent_id ORDER BY csat_plus_requester_id DESC, id DESC) AS recency_rank
        FROM valid_tickets
    ) ranked
    WHERE recency_rank = 1
)
SELECT
    csat,
    agent_id,
    GROUPING(csat) AS is_total_csat,
    COUNT(*) AS n_rows,
    ROUND(AVG(csat_plus_requester_id), 2) AS avg_csat_plus_requester_id,
    MAX(closed_at) AS last_closed_at
FROM deduped
GROUP BY ROLLUP(csat, agent_id)
ORDER BY csat NULLS LAST, agent_id NULLS LAST
