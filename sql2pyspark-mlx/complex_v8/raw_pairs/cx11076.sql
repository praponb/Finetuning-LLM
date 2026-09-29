WITH recent AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        channel,
        csat,
        tags
    FROM tickets
    WHERE agent_id IS NOT NULL
      AND channel <> 'chat'
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        tags,
        COUNT(*) OVER (PARTITION BY csat, agent_id) AS rows_in_csat_agent_id,
        SIZE(COLLECT_SET(channel) OVER (PARTITION BY csat, agent_id)) AS distinct_channel_in_csat_agent_id,
        DENSE_RANK() OVER (PARTITION BY csat ORDER BY csat DESC, id) AS csat_dense_rank
    FROM recent
),
combined_details_agent AS (
    -- Join support_agents
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.opened_at,
        s.tags,
        s.rows_in_csat_agent_id,
        s.distinct_channel_in_csat_agent_id,
        su.agent_name
    FROM sequenced s
    LEFT JOIN support_agents su ON su.id = s.agent_id
),
enriched AS (
    -- Attach agent attributes
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.opened_at,
        s.rows_in_csat_agent_id,
        s.distinct_channel_in_csat_agent_id,
        s.agent_name,
        su.tier AS agent_tier,
        su.agent_name AS agent_agent_name
    FROM combined_details_agent s
    JOIN support_agents su ON su.id = s.agent_id AND su.hired_on >= DATE '2024-05-02'
),
annotated AS (
    -- Label and score rows
    SELECT
        *,
        opened_at + INTERVAL 30 MINUTES AS opened_at_plus_30m,
        INITCAP(TRIM(agent_tier)) AS agent_tier_clean
    FROM enriched
),
grouped AS (
    -- Group-level totals
    SELECT
        DATE_FORMAT(opened_at, 'yyyy-MM') AS opened_at_ym,
        SUM(CASE WHEN agent_agent_name IS NOT NULL THEN 1 ELSE 0 END) AS n_agent_agent_name_is_not_null,
        MAX(opened_at) AS last_opened_at
    FROM annotated
    WHERE rows_in_csat_agent_id >= 4
    GROUP BY DATE_FORMAT(opened_at, 'yyyy-MM')
),
tagged AS (
    -- Label and score rows
    SELECT *, SUBSTRING(TRIM(opened_at_ym), 1, 4) AS opened_at_ym_prefix
    FROM grouped
    WHERE last_opened_at >= TIMESTAMP '2025-01-19 07:00:00'
)
-- Final projection
SELECT opened_at_ym, last_opened_at, opened_at_ym_prefix
FROM tagged
