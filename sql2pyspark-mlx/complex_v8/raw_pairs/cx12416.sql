WITH cleaned AS (
    -- Keep valid rows and derive helper columns
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
    WHERE priority IN ('P4', 'P1', 'P3')
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        MIN(csat) OVER (PARTITION BY TO_DATE(opened_at), agent_id) AS opened_at_to_date_agent_id_min_csat,
        ROUND(AVG(csat) OVER w_opened_at_to_date_agent_id, 2) AS opened_at_to_date_agent_id_avg_csat,
        ROUND(requester_id - AVG(requester_id) OVER (PARTITION BY TO_DATE(opened_at), agent_id), 2) AS requester_id_vs_opened_at_to_date_agent_id_avg,
        ROUND(CUME_DIST() OVER (PARTITION BY priority ORDER BY csat), 3) AS csat_cume_dist,
        ROUND(csat / NULLIF(SUM(csat) OVER w_opened_at_to_date_agent_id, 0), 4) AS csat_share_of_opened_at_to_date_agent_id
    FROM cleaned
    WINDOW w_opened_at_to_date_agent_id AS (PARTITION BY TO_DATE(opened_at), agent_id)
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        SIZE(COLLECT_SET(channel) OVER w_agent_id) AS distinct_channel_in_agent_id,
        ROUND(AVG(requester_id) OVER (PARTITION BY agent_id), 2) AS agent_id_avg_requester_id,
        NTH_VALUE(requester_id, 2) OVER (PARTITION BY csat ORDER BY csat_cume_dist, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_requester_id,
        MAX(csat) OVER (PARTITION BY agent_id) AS agent_id_max_csat,
        ROUND(csat_cume_dist / NULLIF(SUM(csat_cume_dist) OVER w_agent_id, 0), 4) AS csat_cume_dist_share_of_agent_id,
        ROW_NUMBER() OVER (PARTITION BY csat ORDER BY csat_cume_dist, id) AS seq_no
    FROM windowed
    WINDOW w_agent_id AS (PARTITION BY agent_id)
),
with_parent_agent AS (
    -- Attach agent attributes
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.opened_at,
        s.closed_at,
        s.priority,
        s.channel,
        s.tags,
        s.opened_at_to_date_agent_id_min_csat,
        s.opened_at_to_date_agent_id_avg_csat,
        s.requester_id_vs_opened_at_to_date_agent_id_avg,
        s.csat_share_of_opened_at_to_date_agent_id,
        s.distinct_channel_in_agent_id,
        s.agent_id_avg_requester_id,
        s.second_requester_id,
        s.agent_id_max_csat,
        s.csat_cume_dist_share_of_agent_id,
        su.agent_name,
        su.tier AS agent_tier,
        su.team AS agent_team
    FROM with_trends s
    JOIN support_agents su ON su.id = s.agent_id
),
with_trends_2 AS (
    -- Window metrics over several partitions
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        tags,
        opened_at_to_date_agent_id_min_csat,
        opened_at_to_date_agent_id_avg_csat,
        requester_id_vs_opened_at_to_date_agent_id_avg,
        distinct_channel_in_agent_id,
        agent_id_avg_requester_id,
        second_requester_id,
        agent_id_max_csat,
        csat_cume_dist_share_of_agent_id,
        NTH_VALUE(opened_at, 2) OVER (PARTITION BY agent_tier, agent_id ORDER BY csat_share_of_opened_at_to_date_agent_id DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_opened_at,
        SIZE(COLLECT_SET(agent_id) OVER w_agent_tier_channel) AS distinct_agent_id_in_agent_tier_channel,
        NTILE(3) OVER (PARTITION BY agent_tier, agent_id ORDER BY csat_share_of_opened_at_to_date_agent_id DESC, id) AS csat_share_of_opened_at_to_date_agent_id_tercile
    FROM with_parent_agent
    WINDOW w_agent_tier_channel AS (PARTITION BY agent_tier, channel)
),
priority_stats AS (
    -- Avg agent_id_max_csat per priority
    SELECT priority, AVG(agent_id_max_csat) AS avg_agent_id_max_csat
    FROM with_trends_2
    GROUP BY priority
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_agent_id_max_csat,
        ROUND(s.agent_id_max_csat - g.avg_agent_id_max_csat, 2) AS agent_id_max_csat_minus_avg
    FROM with_trends_2 s
    JOIN priority_stats g ON g.priority = s.priority
    WHERE s.agent_id_max_csat > g.avg_agent_id_max_csat
)
SELECT DISTINCT
    tags,
    opened_at_to_date_agent_id_min_csat,
    distinct_agent_id_in_agent_tier_channel
FROM outliers
WHERE avg_agent_id_max_csat < 35.3
