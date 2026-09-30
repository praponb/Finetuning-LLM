WITH recent AS (
    -- Filter tickets
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        csat,
        tags,
        ROUND((UNIX_TIMESTAMP(closed_at) - UNIX_TIMESTAMP(opened_at)) / 3600, 1) AS hours_opened_at_to_closed_at,
        DATE_FORMAT(opened_at, 'yyyy-MM-dd HH:00') AS opened_at_label
    FROM tickets
),
combined_details_agent AS (
    -- Attach agent attributes
    SELECT
        s.id,
        s.agent_id,
        s.opened_at,
        s.csat,
        s.tags,
        s.hours_opened_at_to_closed_at,
        s.opened_at_label,
        su.team AS agent_team
    FROM recent s
    LEFT JOIN support_agents su ON su.id = s.agent_id
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LEAD(hours_opened_at_to_closed_at, 2, 0) OVER (ORDER BY csat NULLS LAST, id) AS next2_hours_opened_at_to_closed_at,
        SIZE(COLLECT_SET(agent_id) OVER (PARTITION BY csat, agent_team)) AS distinct_agent_id_in_csat_agent_team,
        ROUND(hours_opened_at_to_closed_at / NULLIF(SUM(hours_opened_at_to_closed_at) OVER (PARTITION BY agent_team, agent_id), 0), 4) AS hours_opened_at_to_closed_at_share_of_agent_team,
        DENSE_RANK() OVER w_csat_by_hours_opened_at_to_c AS hours_opened_at_to_closed_at_dense_rank,
        FIRST_VALUE(csat) IGNORE NULLS OVER (PARTITION BY csat ORDER BY hours_opened_at_to_closed_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_csat,
        ROUND(AVG(csat) OVER (ORDER BY hours_opened_at_to_closed_at ASC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS csat_avg_3,
        DENSE_RANK() OVER (ORDER BY hours_opened_at_to_closed_at ASC) AS hours_opened_at_to_closed_at_dense_rank_2,
        LEAD(csat, 1, 0) OVER (ORDER BY csat NULLS LAST, id) AS next_csat,
        LEAD(hours_opened_at_to_closed_at, 3, 0) OVER (ORDER BY hours_opened_at_to_closed_at ASC, id) AS next3_hours_opened_at_to_closed_at,
        NTILE(4) OVER w_all_by_csat AS csat_quartile,
        MAX(hours_opened_at_to_closed_at) OVER (ORDER BY hours_opened_at_to_closed_at, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS hours_opened_at_to_closed_at_max_6_prior,
        MAX(hours_opened_at_to_closed_at) OVER (ORDER BY csat NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_hours_opened_at_to_closed_at_so_far
    FROM combined_details_agent
    WINDOW w_csat_by_hours_opened_at_to_c AS (PARTITION BY csat ORDER BY hours_opened_at_to_closed_at DESC), w_all_by_csat AS (ORDER BY csat NULLS LAST, id)
),
top_per_group AS (
    -- First row per group
    SELECT *
    FROM metrics
    WHERE hours_opened_at_to_closed_at_dense_rank_2 = 1
),
metrics_2 AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(CUME_DIST() OVER w_hours_opened_at_to_closed_at, 3) AS hours_opened_at_to_closed_at_cume_dist,
        SUM(next_csat) OVER (PARTITION BY DATE_TRUNC('MONTH', opened_at), agent_id ORDER BY hours_opened_at_to_closed_at_dense_rank_2 DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS next_csat_sum_6_prior,
        ROUND(PERCENT_RANK() OVER (PARTITION BY hours_opened_at_to_closed_at_dense_rank_2, hours_opened_at_to_closed_at_dense_rank ORDER BY hours_opened_at_to_closed_at), 3) AS hours_opened_at_to_closed_at_percent_rank
    FROM top_per_group
    WINDOW w_hours_opened_at_to_closed_at AS (PARTITION BY hours_opened_at_to_closed_at_dense_rank_2, hours_opened_at_to_closed_at_dense_rank ORDER BY hours_opened_at_to_closed_at, id)
)
-- Final projection
SELECT
    id,
    agent_id,
    opened_at,
    csat,
    tags,
    opened_at_label,
    agent_team,
    next2_hours_opened_at_to_closed_at,
    distinct_agent_id_in_csat_agent_team,
    hours_opened_at_to_closed_at_dense_rank,
    first_known_csat,
    csat_avg_3,
    next3_hours_opened_at_to_closed_at,
    hours_opened_at_to_closed_at_max_6_prior,
    max_hours_opened_at_to_closed_at_so_far,
    hours_opened_at_to_closed_at_cume_dist,
    hours_opened_at_to_closed_at_percent_rank,
    CONCAT_WS('|', opened_at_label, agent_team) AS opened_at_label_agent_team_label
FROM metrics_2
WHERE opened_at_label IS NOT NULL
ORDER BY opened_at, agent_id NULLS LAST
