WITH scoped_tickets AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        csat,
        tags,
        CONCAT(priority, '#', CAST(csat AS STRING)) AS priority_ref,
        CASE WHEN csat >= 4 THEN 'large' WHEN csat >= 2 THEN 'medium' ELSE 'small' END AS csat_tier
    FROM tickets
    WHERE TO_DATE(closed_at) < DATE '2025-02-04'
),
deduped AS (
    -- Keep the latest row per priority
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        csat,
        tags,
        priority_ref,
        csat_tier
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY priority ORDER BY closed_at DESC, id DESC) AS row_num
        FROM scoped_tickets
    ) ranked
    WHERE row_num = 1
),
with_activity AS (
    -- Rows with no matching tickets
    SELECT s.*
    FROM deduped s
    WHERE NOT EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.agent_id = s.agent_id
              AND t.priority NOT IN ('P1', 'P2')
              AND SIZE(t.tags) >= 2
        )
)
-- Final projection
SELECT
    id,
    requester_id,
    opened_at,
    priority,
    channel,
    csat,
    tags,
    priority_ref,
    csat_tier,
    ABS(requester_id - 18) AS requester_id_distance
FROM with_activity
WHERE channel IN ('web', 'phone', 'chat')
