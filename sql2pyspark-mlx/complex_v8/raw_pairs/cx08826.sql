WITH scoped AS (
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
    WHERE ARRAY_CONTAINS(tags, 'outage')
),
channel_stats AS (
    -- Avg csat per channel
    SELECT channel, AVG(csat) AS avg_csat
    FROM scoped
    GROUP BY channel
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.avg_csat, ROUND(s.csat - g.avg_csat, 2) AS csat_minus_avg
    FROM scoped s
    JOIN channel_stats g ON g.channel = s.channel
    WHERE s.csat > g.avg_csat
),
per_label AS (
    -- One row per tag
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.opened_at,
        s.closed_at,
        s.priority,
        s.channel,
        s.csat,
        s.avg_csat,
        s.csat_minus_avg,
        t.tag
    FROM vs_group s
    LATERAL VIEW EXPLODE(s.tags) t AS tag
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY csat) AS rows_in_csat,
        ROUND(CUME_DIST() OVER (PARTITION BY YEAR(closed_at), agent_id ORDER BY opened_at DESC), 3) AS opened_at_cume_dist,
        ROUND(csat / NULLIF(SUM(csat) OVER (PARTITION BY csat), 0), 4) AS csat_share_of_csat,
        ROUND(PERCENT_RANK() OVER (PARTITION BY YEAR(closed_at), agent_id ORDER BY opened_at DESC, id, tag), 3) AS opened_at_percent_rank,
        MIN(avg_csat) OVER w_csat AS csat_min_avg_csat
    FROM per_label
    WINDOW w_csat AS (PARTITION BY csat)
),
ticket_stats AS (
    -- Per-agent tickets metrics
    SELECT
        agent_id,
        MAX(opened_at) AS last_ticket_opened_at,
        MAX(closed_at) AS last_ticket_closed_at,
        MAX(opened_at) AS last_ticket_opened_at_2
    FROM tickets
    GROUP BY agent_id
),
summarized_rows AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        t.last_ticket_opened_at,
        t.last_ticket_closed_at,
        t.last_ticket_opened_at_2
    FROM windowed s
    LEFT JOIN ticket_stats t ON t.agent_id = s.agent_id
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        su.agent_name,
        su.tier AS agent_tier,
        su.hired_on AS agent_hired_on
    FROM summarized_rows s
    LEFT JOIN support_agents su ON su.id = s.agent_id
),
ticket_stats_2 AS (
    -- Per-agent tickets metrics
    SELECT
        agent_id,
        MIN(closed_at) AS first_ticket_closed_at,
        ROUND(AVG(csat), 2) AS avg_ticket_csat,
        COUNT(*) FILTER (WHERE channel IS NOT NULL) AS ticket_channel_count
    FROM tickets
    GROUP BY agent_id
),
with_stats AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        t.first_ticket_closed_at,
        t.avg_ticket_csat,
        COALESCE(t.ticket_channel_count, 0) AS ticket_channel_count
    FROM combined_details s
    LEFT JOIN ticket_stats_2 t ON t.agent_id = s.agent_id
)
-- Final projection
SELECT
    id,
    requester_id,
    agent_id,
    opened_at,
    channel,
    csat,
    avg_csat,
    csat_minus_avg,
    tag,
    rows_in_csat,
    opened_at_cume_dist,
    csat_share_of_csat,
    last_ticket_closed_at,
    last_ticket_opened_at_2,
    agent_name,
    agent_tier,
    first_ticket_closed_at,
    avg_ticket_csat,
    ticket_channel_count,
    SUBSTRING(TRIM(agent_tier), 1, 4) AS agent_tier_prefix
FROM with_stats
WHERE opened_at_percent_rank >= 0.2
