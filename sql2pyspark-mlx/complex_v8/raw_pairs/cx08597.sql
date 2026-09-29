WITH filtered AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        channel,
        csat,
        CASE WHEN requester_id >= 22 THEN 'large' WHEN requester_id >= 9 THEN 'medium' ELSE 'small' END AS requester_id_tier,
        CASE WHEN requester_id >= 19 THEN 'large' WHEN requester_id >= 7 THEN 'medium' ELSE 'small' END AS requester_id_tier_2
    FROM tickets
    WHERE TO_DATE(opened_at) < DATE '2025-02-12'
),
without_matches AS (
    -- Rows with no matching tickets
    SELECT s.*
    FROM filtered s
    WHERE NOT EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.agent_id = s.agent_id
              AND t.priority IN ('P4', 'P1')
        )
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, field, metric_value
FROM (
    SELECT id, csat, requester_id
    FROM without_matches
) src
UNPIVOT (
    metric_value FOR field IN (csat, requester_id)
) unp
