WITH recent AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        channel,
        csat,
        tags,
        GREATEST(requester_id, csat) AS greatest_requester_id_csat,
        CASE WHEN channel IN ('chat', 'email') THEN 'group_a' WHEN channel = 'phone' THEN 'group_b' ELSE 'other' END AS channel_group
    FROM tickets
    WHERE closed_at >= TIMESTAMP '2025-01-27 00:00:00'
      AND HOUR(opened_at) BETWEEN 9 AND 14
),
with_activity AS (
    -- Rows with no matching ticket_events
    SELECT s.*
    FROM recent s
    WHERE NOT EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.id
              AND t.event_type = 'replied'
        )
      AND s.channel_group = 'group_b'
),
rollup_base AS (
    -- Aggregate with conditional metrics
    SELECT
        agent_id,
        ROUND(SUM(greatest_requester_id_csat) / NULLIF(SUM(csat), 0), 3) AS greatest_requester_id_csat_to_csat_ratio,
        MIN(requester_id) AS min_requester_id
    FROM with_activity
    GROUP BY 1
    HAVING SUM(greatest_requester_id_csat) > 7
)
-- Final projection
SELECT agent_id, greatest_requester_id_csat_to_csat_ratio, min_requester_id
FROM rollup_base
WHERE min_requester_id > 11
