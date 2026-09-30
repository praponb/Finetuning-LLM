WITH prepared_tickets AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        priority,
        channel,
        GREATEST(requester_id, csat) AS greatest_requester_id_csat,
        CASE WHEN requester_id >= 20 THEN 'A' WHEN requester_id >= 7 THEN 'B' ELSE 'C' END AS requester_id_tier
    FROM tickets
),
derived AS (
    -- Add calculated columns
    SELECT
        *,
        CASE WHEN requester_id_tier IN ('A', 'B') THEN 'group_a' WHEN requester_id_tier = 'C' THEN 'group_b' ELSE 'other' END AS requester_id_tier_group,
        CASE WHEN requester_id >= 18 THEN 'HIGH' WHEN requester_id >= 6 THEN 'MEDIUM' ELSE 'LOW' END AS requester_id_tier_2,
        CASE WHEN channel IN ('email', 'chat') THEN 'group_a' WHEN channel = 'web' THEN 'group_b' ELSE 'other' END AS channel_group
    FROM prepared_tickets
)
SELECT
    COALESCE(channel_group, 'ALL') AS channel_group,
    COALESCE(requester_id_tier, 'ALL') AS requester_id_tier,
    SUM(requester_id) AS total_requester_id,
    ARRAY_SORT(COLLECT_SET(channel)) AS channel_list
FROM derived
GROUP BY GROUPING SETS ((channel_group, requester_id_tier), (channel_group), (requester_id_tier), ())
