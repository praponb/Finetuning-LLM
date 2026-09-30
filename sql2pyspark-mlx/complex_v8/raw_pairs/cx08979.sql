WITH base AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        csat,
        tags
    FROM tickets
    WHERE csat < 4
),
stats AS (
    -- Group-level totals
    SELECT
        channel,
        SUM(csat) AS total_csat,
        ROUND(SUM(requester_id) / NULLIF(SUM(csat), 0), 3) AS requester_id_to_csat_ratio,
        ROUND(AVG(csat), 2) AS avg_csat,
        SUM(requester_id) AS total_requester_id
    FROM base
    GROUP BY 1
),
windowed AS (
    -- Previous / next values per key
    SELECT
        channel,
        total_csat,
        requester_id_to_csat_ratio,
        total_requester_id,
        ROUND(total_csat - AVG(total_csat) OVER (PARTITION BY channel), 2) AS total_csat_vs_channel_avg,
        RANK() OVER (ORDER BY total_requester_id DESC, channel) AS total_requester_id_rank
    FROM stats
),
top_per_group AS (
    -- Top 5 per group
    SELECT
        channel,
        total_csat,
        requester_id_to_csat_ratio,
        total_requester_id,
        total_csat_vs_channel_avg
    FROM windowed
    WHERE total_requester_id_rank <= 5
)
-- Final projection
SELECT
    channel,
    requester_id_to_csat_ratio,
    total_csat_vs_channel_avg,
    LOWER(REGEXP_REPLACE(TRIM(channel), ' ', '_')) AS channel_slug
FROM top_per_group
WHERE channel = 'phone'
ORDER BY channel DESC, requester_id_to_csat_ratio
