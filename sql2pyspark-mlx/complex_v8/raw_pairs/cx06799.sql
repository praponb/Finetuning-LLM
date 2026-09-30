WITH scoped AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        priority,
        channel,
        tags,
        csat + requester_id AS csat_plus_requester_id,
        DATE_FORMAT(closed_at, 'yyyy-MM-dd HH:00') AS closed_at_label,
        TO_DATE(closed_at) AS closed_at_date
    FROM tickets
    WHERE MONTH(opened_at) IN (1, 2)
),
qualified AS (
    -- Rows that have matching tickets
    SELECT s.*
    FROM scoped s
    WHERE EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.agent_id = s.agent_id
              AND SIZE(t.tags) >= 1
              AND t.channel = 'web'
        )
),
qualified_2 AS (
    -- Compare against the overall avg
    SELECT *
    FROM qualified
    WHERE requester_id > (SELECT AVG(requester_id) FROM qualified)
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        requester_id,
        opened_at,
        priority,
        closed_at_label,
        closed_at_date,
        ROUND(requester_id - AVG(requester_id) OVER (PARTITION BY agent_id), 2) AS requester_id_vs_agent_id_avg,
        ROUND(csat_plus_requester_id / NULLIF(SUM(csat_plus_requester_id) OVER (PARTITION BY agent_id), 0), 4) AS csat_plus_requester_id_share_of_agent_id,
        LAG(opened_at, 2) OVER (PARTITION BY agent_id, channel ORDER BY requester_id NULLS LAST, id) AS prev2_opened_at,
        ROUND(csat_plus_requester_id - AVG(csat_plus_requester_id) OVER w_agent_id, 2) AS csat_plus_requester_id_vs_agent_id_avg
    FROM qualified_2
    WINDOW w_agent_id AS (PARTITION BY agent_id)
)
-- Final projection
SELECT
    id,
    opened_at,
    closed_at_label,
    closed_at_date,
    requester_id_vs_agent_id_avg,
    prev2_opened_at,
    csat_plus_requester_id_vs_agent_id_avg,
    CONCAT(closed_at_label, '#', CAST(requester_id AS STRING)) AS closed_at_label_ref
FROM metrics
ORDER BY csat_plus_requester_id_vs_agent_id_avg
