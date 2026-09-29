WITH prepared_tickets AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        agent_id,
        opened_at,
        channel,
        tags,
        LOWER(REGEXP_REPLACE(TRIM(priority), ' ', '_')) AS priority_slug,
        LEAST(csat, requester_id) AS least_csat_requester_id,
        HOUR(closed_at) AS closed_at_hour
    FROM tickets
    WHERE opened_at < TIMESTAMP '2025-02-18 07:00:00'
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(AVG(least_csat_requester_id) OVER (PARTITION BY YEAR(opened_at)), 2) AS opened_at_year_avg_least_csat_requester_id,
        ROUND(AVG(closed_at_hour) OVER w_opened_at_year, 2) AS opened_at_year_avg_closed_at_hour,
        ROUND(least_csat_requester_id / NULLIF(SUM(least_csat_requester_id) OVER (PARTITION BY YEAR(opened_at)), 0), 4) AS least_csat_requester_id_share_of_opened_at_year,
        SIZE(COLLECT_SET(channel) OVER (PARTITION BY YEAR(opened_at))) AS distinct_channel_in_opened_at_year
    FROM prepared_tickets
    WINDOW w_opened_at_year AS (PARTITION BY YEAR(opened_at))
),
joined AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.agent_id,
        s.channel,
        s.tags,
        s.priority_slug,
        s.least_csat_requester_id,
        s.opened_at_year_avg_least_csat_requester_id,
        s.opened_at_year_avg_closed_at_hour,
        s.least_csat_requester_id_share_of_opened_at_year,
        s.distinct_channel_in_opened_at_year,
        su.tier AS agent_tier,
        su.team AS agent_team
    FROM with_trends s
    JOIN support_agents su ON su.id = s.agent_id
),
agent_tier_stats AS (
    -- Avg least_csat_requester_id_share_of_opened_at_year per agent_tier
    SELECT agent_tier, AVG(least_csat_requester_id_share_of_opened_at_year) AS avg_least_csat_requester_id_share_of_opened_at_y
    FROM joined
    GROUP BY agent_tier
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_least_csat_requester_id_share_of_opened_at_y,
        ROUND(s.least_csat_requester_id_share_of_opened_at_year - g.avg_least_csat_requester_id_share_of_opened_at_y, 2) AS least_csat_requester_id_share_of_opened_at_year_
    FROM joined s
    JOIN agent_tier_stats g ON g.agent_tier = s.agent_tier
    WHERE s.least_csat_requester_id_share_of_opened_at_year > g.avg_least_csat_requester_id_share_of_opened_at_y
)
-- Final projection
SELECT
    id,
    agent_id,
    channel,
    priority_slug,
    least_csat_requester_id,
    opened_at_year_avg_closed_at_hour,
    least_csat_requester_id_share_of_opened_at_year,
    distinct_channel_in_opened_at_year,
    agent_tier,
    avg_least_csat_requester_id_share_of_opened_at_y,
    least_csat_requester_id_share_of_opened_at_year_,
    INITCAP(TRIM(priority_slug)) AS priority_slug_clean,
    ARRAY_CONTAINS(tags, 'upgrade') AS has_upgrade
FROM above_average
ORDER BY opened_at_year_avg_closed_at_hour DESC
