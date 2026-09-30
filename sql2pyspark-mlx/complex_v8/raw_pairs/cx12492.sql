WITH cleaned AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        priority,
        channel,
        csat,
        csat - requester_id AS csat_minus_requester_id,
        FLOOR(requester_id / 5) * 5 AS requester_id_bucket,
        LOWER(REGEXP_REPLACE(TRIM(priority), ' ', '_')) AS priority_slug
    FROM tickets
    WHERE SIZE(tags) >= 2
      AND agent_id IS NOT NULL
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        csat_minus_requester_id - LAG(csat_minus_requester_id) OVER w_channel_by_requester_id_buck AS csat_minus_requester_id_change,
        NTH_VALUE(requester_id, 2) OVER (PARTITION BY priority, agent_id ORDER BY requester_id, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_requester_id
    FROM cleaned
    WINDOW w_channel_by_requester_id_buck AS (PARTITION BY channel ORDER BY requester_id_bucket, id)
),
profiled AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM tickets t WHERE t.agent_id = s.agent_id) AS ticket_count
    FROM with_trends s
),
tagged AS (
    -- Label and score rows
    SELECT
        *,
        INITCAP(TRIM(channel)) AS channel_clean,
        ROUND(requester_id / NULLIF(requester_id_bucket, 0), 3) AS requester_id_per_requester_id_bucket,
        LENGTH(TRIM(channel)) AS channel_len
    FROM profiled
)
-- Final projection
SELECT
    id,
    requester_id,
    closed_at,
    priority,
    channel,
    csat,
    requester_id_bucket,
    priority_slug,
    csat_minus_requester_id_change,
    second_requester_id,
    ticket_count,
    channel_len,
    ROUND(requester_id_bucket / NULLIF(channel_len, 0), 3) AS requester_id_bucket_per_channel_len,
    CONCAT(channel, '#', CAST(ticket_count AS STRING)) AS channel_ref
FROM tagged
ORDER BY csat, channel_ref
