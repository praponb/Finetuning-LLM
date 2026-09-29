WITH filtered_tickets AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        GREATEST(requester_id, csat) AS greatest_requester_id_csat,
        ROUND((UNIX_TIMESTAMP(opened_at) - UNIX_TIMESTAMP(closed_at)) / 3600, 1) AS hours_closed_at_to_opened_at,
        YEAR(opened_at) AS opened_at_year
    FROM tickets
    WHERE requester_id > 4
),
with_parent AS (
    -- Enrich with dimension data
    SELECT s.*, su.tier AS agent_tier, su.team AS agent_team
    FROM filtered_tickets s
    LEFT JOIN support_agents su ON su.id = s.agent_id
),
windowed AS (
    -- Previous / next values per key
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        greatest_requester_id_csat,
        hours_closed_at_to_opened_at,
        opened_at_year,
        agent_tier,
        agent_team,
        LAST_VALUE(hours_closed_at_to_opened_at) IGNORE NULLS OVER (PARTITION BY agent_id ORDER BY opened_at_year ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_hours_closed_at_to_opened_at,
        MIN(hours_closed_at_to_opened_at) OVER (PARTITION BY agent_id, priority) AS agent_id_priority_min_hours_closed_at_to_opened_,
        LAST_VALUE(opened_at_year) OVER (PARTITION BY agent_id ORDER BY opened_at_year ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_opened_at_year,
        ROUND(CUME_DIST() OVER w_agent_id_by_opened_at_year, 3) AS opened_at_year_cume_dist,
        MAX(requester_id) OVER (PARTITION BY agent_id, priority) AS agent_id_priority_max_requester_id,
        ROUND(PERCENT_RANK() OVER (PARTITION BY agent_id ORDER BY opened_at_year ASC), 3) AS opened_at_year_percent_rank
    FROM with_parent
    WINDOW w_agent_id_by_opened_at_year AS (PARTITION BY agent_id ORDER BY opened_at_year ASC, id)
),
agent_tier_stats AS (
    -- Median agent_id_priority_min_hours_closed_at_to_opened_ per agent_tier
    SELECT agent_tier, PERCENTILE_APPROX(agent_id_priority_min_hours_closed_at_to_opened_, 0.5) AS median_agent_id_priority_min_hours_closed_at_to_
    FROM windowed
    GROUP BY agent_tier
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_agent_id_priority_min_hours_closed_at_to_,
        ROUND(s.agent_id_priority_min_hours_closed_at_to_opened_ - g.median_agent_id_priority_min_hours_closed_at_to_, 2) AS agent_id_priority_min_hours_closed_at_to_opened__2
    FROM windowed s
    JOIN agent_tier_stats g ON g.agent_tier = s.agent_tier
    WHERE s.agent_id_priority_min_hours_closed_at_to_opened_ >= g.median_agent_id_priority_min_hours_closed_at_to_
),
derived AS (
    -- Add calculated columns
    SELECT
        *,
        COALESCE(opened_at_year_cume_dist, 0.0) AS opened_at_year_cume_dist_filled,
        CONCAT(channel, '#', CAST(agent_id AS STRING)) AS channel_ref
    FROM above_average
)
-- Final projection
SELECT
    id,
    agent_id,
    opened_at,
    priority,
    channel,
    hours_closed_at_to_opened_at,
    opened_at_year,
    agent_tier,
    agent_team,
    last_known_hours_closed_at_to_opened_at,
    agent_id_priority_min_hours_closed_at_to_opened_,
    last_opened_at_year,
    agent_id_priority_max_requester_id,
    opened_at_year_percent_rank,
    agent_id_priority_min_hours_closed_at_to_opened__2,
    opened_at_year_cume_dist_filled,
    channel_ref,
    DATE_FORMAT(closed_at, 'yyyy-MM-dd HH:00') AS closed_at_label,
    CONCAT(priority, '#', CAST(id AS STRING)) AS priority_ref
FROM derived
WHERE hours_closed_at_to_opened_at < 1200.0
