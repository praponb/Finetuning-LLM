WITH base_tickets AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        priority,
        channel,
        csat,
        tags,
        COALESCE(channel, 'UNKNOWN') AS channel_or_unknown,
        CASE WHEN csat >= 4 THEN 'HIGH' WHEN csat >= 3 THEN 'MEDIUM' ELSE 'LOW' END AS csat_tier
    FROM tickets
    WHERE ARRAY_CONTAINS(tags, 'outage')
      AND agent_id >= 4
),
scored AS (
    -- Label and score rows
    SELECT
        *,
        CONCAT(csat_tier, '#', CAST(agent_id AS STRING)) AS csat_tier_ref,
        SUBSTRING(TRIM(priority), 1, 4) AS priority_prefix
    FROM base_tickets
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(csat / NULLIF(SUM(csat) OVER (PARTITION BY agent_id, priority), 0), 4) AS csat_share_of_agent_id_priority,
        ROUND(PERCENT_RANK() OVER w_priority_by_csat_ties, 3) AS csat_percent_rank,
        ROW_NUMBER() OVER (PARTITION BY priority ORDER BY csat, id) AS seq_no,
        requester_id - LAG(requester_id) OVER (PARTITION BY priority ORDER BY csat, id) AS requester_id_change,
        ROUND(AVG(requester_id) OVER (PARTITION BY priority ORDER BY csat, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS requester_id_avg_2,
        LEAD(requester_id, 2) OVER (PARTITION BY priority ORDER BY csat, id) AS next2_requester_id
    FROM scored
    WINDOW w_priority_by_csat_ties AS (PARTITION BY priority ORDER BY csat)
)
-- Final projection
SELECT
    id,
    requester_id,
    priority,
    csat,
    tags,
    channel_or_unknown,
    csat_tier,
    csat_tier_ref,
    priority_prefix,
    csat_percent_rank,
    seq_no,
    requester_id_change,
    requester_id_avg_2,
    next2_requester_id,
    requester_id + requester_id_avg_2 AS requester_id_plus_requester_id_avg_2,
    LEAST(requester_id_change, next2_requester_id) AS least_requester_id_change_next2_requester_id
FROM with_trends
ORDER BY id DESC, requester_id DESC
