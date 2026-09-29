WITH scoped AS (
    -- Filter tickets
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
    WHERE requester_id >= 8
),
combined_details_agent AS (
    -- Attach agent attributes
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.opened_at,
        s.closed_at,
        s.channel,
        s.csat,
        s.tags,
        su.team AS agent_team,
        su.agent_name,
        su.tier AS agent_tier
    FROM scoped s
    JOIN support_agents su ON su.id = s.agent_id AND su.hired_on BETWEEN DATE '2024-01-30' AND DATE '2025-03-30'
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY agent_id ORDER BY requester_id DESC, id) AS rn,
        LAG(requester_id, 2) OVER w_closed_at_to_date_agent_id_b AS prev2_requester_id,
        MAX(csat) OVER (PARTITION BY agent_team ORDER BY requester_id NULLS LAST, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS csat_max_1_prior,
        NTH_VALUE(closed_at, 2) OVER (ORDER BY csat, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_closed_at,
        ROUND(PERCENT_RANK() OVER (PARTITION BY TO_DATE(closed_at), agent_id ORDER BY csat DESC, id), 3) AS csat_percent_rank,
        ROUND(requester_id - AVG(requester_id) OVER (PARTITION BY agent_team), 2) AS requester_id_vs_agent_team_avg,
        ROUND(CUME_DIST() OVER (PARTITION BY agent_id ORDER BY requester_id DESC, id), 3) AS requester_id_cume_dist,
        LAST_VALUE(opened_at) OVER (ORDER BY csat, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_opened_at,
        FIRST_VALUE(opened_at) IGNORE NULLS OVER (PARTITION BY agent_team ORDER BY requester_id NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_opened_at,
        MIN(requester_id) OVER (PARTITION BY csat) AS csat_min_requester_id,
        SUM(csat) OVER (PARTITION BY csat) AS csat_sum_csat,
        LAG(csat) OVER w_closed_at_to_date_agent_id_b AS prev_csat,
        LEAD(closed_at, 3) OVER (PARTITION BY agent_team ORDER BY requester_id NULLS LAST, id) AS next3_closed_at,
        SIZE(COLLECT_SET(agent_id) OVER (PARTITION BY csat)) AS distinct_agent_id_in_csat
    FROM combined_details_agent
    WINDOW w_closed_at_to_date_agent_id_b AS (PARTITION BY TO_DATE(closed_at), agent_id ORDER BY csat DESC, id)
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        tags,
        agent_team,
        agent_tier,
        prev2_requester_id,
        csat_max_1_prior,
        second_closed_at,
        csat_percent_rank,
        requester_id_cume_dist,
        last_opened_at,
        first_known_opened_at,
        csat_min_requester_id,
        csat_sum_csat,
        prev_csat,
        next3_closed_at,
        distinct_agent_id_in_csat,
        COUNT(*) OVER (PARTITION BY agent_team) AS rows_in_agent_team,
        NTH_VALUE(prev2_requester_id, 2) OVER (PARTITION BY agent_id, rn ORDER BY last_opened_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_prev2_requester_id,
        SIZE(COLLECT_SET(agent_id) OVER (PARTITION BY agent_team)) AS distinct_agent_id_in_agent_team,
        ROUND(CUME_DIST() OVER (PARTITION BY agent_id, rn ORDER BY last_opened_at), 3) AS last_opened_at_cume_dist,
        ROUND(AVG(csat) OVER (PARTITION BY agent_id, rn ORDER BY last_opened_at, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS csat_avg_2
    FROM ranked
)
SELECT
    id,
    requester_id,
    agent_id,
    agent_team,
    agent_tier,
    prev2_requester_id,
    csat_max_1_prior,
    csat_percent_rank,
    requester_id_cume_dist,
    last_opened_at,
    first_known_opened_at,
    csat_min_requester_id,
    csat_sum_csat,
    prev_csat,
    next3_closed_at,
    second_prev2_requester_id,
    csat_avg_2,
    LOWER(REGEXP_REPLACE(TRIM(agent_team), ' ', '_')) AS agent_team_slug,
    DAYOFWEEK(first_known_opened_at) AS first_known_opened_at_dayofweek
FROM with_windows
WHERE SIZE(tags) >= 1
ORDER BY agent_tier NULLS LAST
