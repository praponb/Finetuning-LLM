WITH valid_tickets AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        channel,
        tags,
        ROUND(requester_id / NULLIF(csat, 0), 3) AS requester_id_per_csat,
        DATE_FORMAT(closed_at, 'yyyy-MM-dd') AS closed_at_label
    FROM tickets
    WHERE channel = 'chat'
),
rollup_base AS (
    -- Group-level totals
    SELECT
        channel,
        agent_id,
        SUM(requester_id_per_csat) FILTER (WHERE channel IS NOT NULL) AS requester_id_per_csat_channel_is_not_null,
        ROUND(SUM(requester_id_per_csat) / NULLIF(SUM(requester_id), 0), 3) AS requester_id_per_csat_to_requester_id_ratio,
        ROUND(SUM(requester_id) / NULLIF(SUM(requester_id_per_csat), 0), 3) AS requester_id_to_requester_id_per_csat_ratio,
        COUNT(DISTINCT agent_id) AS distinct_agent_ids,
        SUM(requester_id_per_csat) AS total_requester_id_per_csat,
        COUNT(*) AS cnt
    FROM valid_tickets
    GROUP BY channel, agent_id
    HAVING COUNT(*) >= 2
),
enriched AS (
    -- Add calculated columns
    SELECT
        *,
        SUBSTRING(TRIM(channel), 1, 3) AS channel_prefix,
        CASE WHEN distinct_agent_ids >= 8 THEN 'large' WHEN distinct_agent_ids >= 3 THEN 'medium' ELSE 'small' END AS distinct_agent_ids_tier,
        COALESCE(cnt, 0) AS cnt_filled
    FROM rollup_base
),
ticket_stats AS (
    -- Per-agent tickets metrics
    SELECT
        agent_id,
        MIN(opened_at) AS first_ticket_opened_at,
        SUM(csat) AS total_ticket_csat
    FROM tickets
    GROUP BY agent_id
),
profiled AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        t.first_ticket_opened_at,
        t.total_ticket_csat
    FROM enriched s
    LEFT JOIN ticket_stats t ON t.agent_id = s.agent_id
),
with_parent_agent AS (
    -- Enrich with dimension data
    SELECT
        s.channel,
        s.agent_id,
        s.requester_id_per_csat_to_requester_id_ratio,
        s.distinct_agent_ids,
        s.total_requester_id_per_csat,
        s.channel_prefix,
        s.distinct_agent_ids_tier,
        s.cnt_filled,
        su.tier AS agent_tier,
        su.team AS agent_team
    FROM profiled s
    LEFT JOIN support_agents su ON su.id = s.agent_id
),
distinct_agent_ids_tier_stats AS (
    -- Avg distinct_agent_ids per distinct_agent_ids_tier
    SELECT distinct_agent_ids_tier, AVG(distinct_agent_ids) AS avg_distinct_agent_ids
    FROM with_parent_agent
    GROUP BY distinct_agent_ids_tier
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_distinct_agent_ids,
        ROUND(s.distinct_agent_ids - g.avg_distinct_agent_ids, 2) AS distinct_agent_ids_minus_avg
    FROM with_parent_agent s
    JOIN distinct_agent_ids_tier_stats g ON g.distinct_agent_ids_tier = s.distinct_agent_ids_tier
    WHERE s.distinct_agent_ids > g.avg_distinct_agent_ids
),
tagged AS (
    -- Add calculated columns
    SELECT
        *,
        SUBSTRING(TRIM(agent_tier), 1, 4) AS agent_tier_prefix,
        FLOOR(distinct_agent_ids / 2) * 2 AS distinct_agent_ids_bucket,
        COALESCE(avg_distinct_agent_ids, 0.0) AS avg_distinct_agent_ids_filled
    FROM vs_group
)
SELECT
    channel,
    agent_id,
    requester_id_per_csat_to_requester_id_ratio,
    distinct_agent_ids,
    total_requester_id_per_csat,
    channel_prefix,
    distinct_agent_ids_tier,
    cnt_filled,
    agent_team,
    avg_distinct_agent_ids,
    distinct_agent_ids_minus_avg,
    agent_tier_prefix,
    distinct_agent_ids_bucket,
    avg_distinct_agent_ids_filled
FROM tagged
ORDER BY distinct_agent_ids, distinct_agent_ids_bucket NULLS LAST
