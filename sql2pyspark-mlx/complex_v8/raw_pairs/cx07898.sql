WITH base_tickets AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        channel,
        csat,
        tags,
        UPPER(TRIM(priority)) AS priority_clean,
        CASE WHEN priority IN ('P3', 'P4') THEN 'group_a' WHEN priority = 'P2' THEN 'group_b' ELSE 'other' END AS priority_group
    FROM tickets
    WHERE channel <> 'phone'
      AND opened_at < TIMESTAMP '2025-02-24 08:00:00'
),
with_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.agent_id,
        s.closed_at,
        s.channel,
        s.csat,
        s.tags,
        s.priority_clean,
        su.team AS agent_team,
        su.agent_name
    FROM base_tickets s
    JOIN support_agents su ON su.id = s.agent_id
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        id,
        closed_at,
        csat,
        tags,
        priority_clean,
        agent_name,
        ROUND(csat - AVG(csat) OVER (PARTITION BY channel), 2) AS csat_vs_channel_avg,
        SUM(csat) OVER w_channel AS channel_sum_csat,
        SUM(csat) OVER (PARTITION BY csat, channel ORDER BY csat DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_csat,
        COUNT(*) OVER (PARTITION BY channel) AS rows_in_channel,
        FIRST_VALUE(closed_at) IGNORE NULLS OVER (ORDER BY closed_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_closed_at,
        ROUND(CUME_DIST() OVER (PARTITION BY csat, channel ORDER BY csat DESC), 3) AS csat_cume_dist,
        FIRST_VALUE(csat) OVER (PARTITION BY csat, channel ORDER BY csat DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_csat,
        ROUND((UNIX_TIMESTAMP(closed_at) - UNIX_TIMESTAMP(LAG(closed_at) OVER (ORDER BY closed_at DESC, id))) / 60, 1) AS minutes_since_prev_closed_at,
        NTH_VALUE(csat, 2) OVER (PARTITION BY csat, channel ORDER BY csat DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_csat,
        SIZE(COLLECT_SET(agent_id) OVER (PARTITION BY channel)) AS distinct_agent_id_in_channel,
        SIZE(COLLECT_SET(agent_team) OVER (PARTITION BY channel)) AS distinct_agent_team_in_channel
    FROM with_details
    WINDOW w_channel AS (PARTITION BY channel)
),
tagged AS (
    -- Add calculated columns
    SELECT
        *,
        LOWER(REGEXP_REPLACE(TRIM(agent_name), ' ', '_')) AS agent_name_slug,
        GREATEST(rows_in_channel, csat) AS greatest_rows_in_channel_csat
    FROM with_trends
)
-- Final projection
SELECT
    id,
    closed_at,
    csat,
    agent_name,
    running_csat,
    rows_in_channel,
    first_known_closed_at,
    csat_cume_dist,
    second_csat,
    distinct_agent_id_in_channel,
    agent_name_slug,
    greatest_rows_in_channel_csat,
    SIZE(tags) AS tags_count
FROM tagged
ORDER BY id
