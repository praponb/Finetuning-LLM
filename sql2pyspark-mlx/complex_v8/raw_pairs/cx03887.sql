WITH prepared_tickets AS (
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
        tags,
        LOWER(REGEXP_REPLACE(TRIM(priority), ' ', '_')) AS priority_slug,
        INITCAP(TRIM(channel)) AS channel_clean
    FROM tickets
    WHERE requester_id > 7
      AND priority = 'P2'
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.opened_at,
        s.closed_at,
        s.priority,
        s.csat,
        s.priority_slug,
        s.channel_clean,
        su.team AS agent_team,
        su.agent_name,
        su.hired_on AS agent_hired_on
    FROM prepared_tickets s
    JOIN support_agents su ON su.id = s.agent_id
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(csat / NULLIF(SUM(csat) OVER (PARTITION BY DATE_TRUNC('MONTH', opened_at)), 0), 4) AS csat_share_of_opened_at_month,
        COUNT(*) OVER w_agent_id AS rows_in_agent_id,
        MAX(csat) OVER (PARTITION BY priority) AS priority_max_csat,
        NTILE(4) OVER w_agent_team_closed_at_month_b AS csat_quartile,
        SUM(requester_id) OVER w_agent_id AS agent_id_sum_requester_id,
        SIZE(COLLECT_SET(agent_team) OVER w_agent_id) AS distinct_agent_team_in_agent_id,
        ROUND((UNIX_TIMESTAMP(closed_at) - UNIX_TIMESTAMP(LAG(closed_at) OVER (PARTITION BY csat ORDER BY closed_at DESC NULLS FIRST, id))) / 60, 1) AS minutes_since_prev_closed_at,
        ROUND(CUME_DIST() OVER (PARTITION BY csat ORDER BY closed_at DESC NULLS FIRST, id), 3) AS closed_at_cume_dist,
        NTILE(5) OVER (PARTITION BY csat ORDER BY closed_at DESC NULLS FIRST, id) AS closed_at_quintile,
        csat - LAG(csat) OVER w_csat_by_requester_id AS csat_change,
        COUNT(*) OVER (ORDER BY agent_hired_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        MIN(requester_id) OVER (PARTITION BY DATE_TRUNC('MONTH', opened_at)) AS opened_at_month_min_requester_id,
        COUNT(*) OVER (PARTITION BY priority) AS rows_in_priority,
        ROUND(AVG(requester_id) OVER (PARTITION BY agent_team, MONTH(closed_at) ORDER BY csat, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS requester_id_avg_2_prior,
        SIZE(COLLECT_SET(agent_team) OVER (PARTITION BY priority)) AS distinct_agent_team_in_priority
    FROM combined_details
    WINDOW w_agent_id AS (PARTITION BY agent_id), w_agent_team_closed_at_month_b AS (PARTITION BY agent_team, MONTH(closed_at) ORDER BY csat, id), w_csat_by_requester_id AS (PARTITION BY csat ORDER BY requester_id DESC NULLS FIRST, id)
),
derived AS (
    -- Label and score rows
    SELECT
        *,
        CASE WHEN csat_quartile >= 7 THEN 'HIGH' WHEN csat_quartile >= 5 THEN 'MEDIUM' ELSE 'LOW' END AS csat_quartile_tier,
        ROUND(requester_id_avg_2_prior * 1.5, 2) AS requester_id_avg_2_prior_adj
    FROM with_windows
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        agent_id,
        closed_at,
        priority,
        csat,
        priority_slug,
        channel_clean,
        agent_team,
        priority_max_csat,
        agent_id_sum_requester_id,
        distinct_agent_team_in_agent_id,
        minutes_since_prev_closed_at,
        closed_at_cume_dist,
        closed_at_quintile,
        running_count,
        opened_at_month_min_requester_id,
        rows_in_priority,
        distinct_agent_team_in_priority,
        csat_quartile_tier,
        LAST_VALUE(closed_at) OVER (PARTITION BY csat_quartile, distinct_agent_team_in_agent_id ORDER BY opened_at_month_min_requester_id, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_closed_at,
        COUNT(*) OVER (PARTITION BY MONTH(closed_at), distinct_agent_team_in_agent_id) AS rows_in_closed_at_month_distinct_agent_team_in_a,
        NTILE(5) OVER w_closed_at_quintile_by_distin AS distinct_agent_team_in_priority_quintile,
        SIZE(COLLECT_SET(csat_quartile_tier) OVER (PARTITION BY MONTH(closed_at), distinct_agent_team_in_agent_id)) AS distinct_csat_quartile_tier_in_closed_at_month_d,
        ROUND(CUME_DIST() OVER (PARTITION BY csat_quartile, distinct_agent_team_in_agent_id ORDER BY opened_at_month_min_requester_id, id), 3) AS opened_at_month_min_requester_id_cume_dist
    FROM derived
    WINDOW w_closed_at_quintile_by_distin AS (PARTITION BY closed_at_quintile ORDER BY distinct_agent_team_in_priority NULLS LAST, id)
)
SELECT
    id,
    agent_id,
    closed_at,
    csat,
    priority_slug,
    agent_team,
    agent_id_sum_requester_id,
    distinct_agent_team_in_agent_id,
    minutes_since_prev_closed_at,
    closed_at_cume_dist,
    closed_at_quintile,
    running_count,
    opened_at_month_min_requester_id,
    rows_in_priority,
    csat_quartile_tier,
    last_closed_at,
    rows_in_closed_at_month_distinct_agent_team_in_a,
    distinct_agent_team_in_priority_quintile,
    distinct_csat_quartile_tier_in_closed_at_month_d,
    opened_at_month_min_requester_id_cume_dist
FROM metrics
ORDER BY distinct_agent_team_in_agent_id
