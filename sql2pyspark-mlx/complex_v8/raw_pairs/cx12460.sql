WITH recent_tickets AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        channel,
        tags,
        requester_id - csat AS requester_id_minus_csat
    FROM tickets
    WHERE MONTH(closed_at) IN (1, 3)
),
joined_agent AS (
    -- Attach agent attributes
    SELECT s.*, su.agent_name, su.team AS agent_team
    FROM recent_tickets s
    JOIN support_agents su ON su.id = s.agent_id
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        requester_id_minus_csat - LAG(requester_id_minus_csat) OVER w_agent_id_agent_team_by_reque AS requester_id_minus_csat_change,
        DENSE_RANK() OVER (PARTITION BY agent_id ORDER BY requester_id DESC) AS requester_id_dense_rank,
        ROUND(requester_id_minus_csat / NULLIF(SUM(requester_id_minus_csat) OVER w_closed_at_year_agent_team, 0), 4) AS requester_id_minus_csat_share_of_closed_at_year_,
        MIN(requester_id) OVER w_closed_at_year_agent_team AS closed_at_year_agent_team_min_requester_id,
        RANK() OVER w_channel_closed_at_month_by_c AS closed_at_rank,
        FIRST_VALUE(requester_id_minus_csat) IGNORE NULLS OVER (PARTITION BY agent_id, agent_team ORDER BY requester_id DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_requester_id_minus_csat,
        SIZE(COLLECT_SET(agent_team) OVER (PARTITION BY agent_id, channel)) AS distinct_agent_team_in_agent_id_channel,
        requester_id - LAG(requester_id) OVER w_channel_closed_at_month_by_c AS requester_id_change,
        COUNT(*) OVER (PARTITION BY agent_id, agent_team ORDER BY requester_id DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        LAG(requester_id, 3) OVER (PARTITION BY agent_id, agent_team ORDER BY requester_id DESC, id) AS prev3_requester_id,
        FIRST_VALUE(requester_id_minus_csat) IGNORE NULLS OVER (PARTITION BY agent_id ORDER BY closed_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_requester_id_minus_csat_2,
        SUM(requester_id_minus_csat) OVER (PARTITION BY agent_id, channel) AS agent_id_channel_sum_requester_id_minus_csat,
        DENSE_RANK() OVER (PARTITION BY agent_team ORDER BY requester_id_minus_csat DESC NULLS FIRST) AS requester_id_minus_csat_dense_rank
    FROM joined_agent
    WINDOW w_agent_id_agent_team_by_reque AS (PARTITION BY agent_id, agent_team ORDER BY requester_id DESC, id), w_closed_at_year_agent_team AS (PARTITION BY YEAR(closed_at), agent_team), w_channel_closed_at_month_by_c AS (PARTITION BY channel, DATE_TRUNC('MONTH', closed_at) ORDER BY closed_at DESC, id)
),
top_per_group AS (
    -- Top 5 per group
    SELECT *
    FROM with_windows
    WHERE requester_id_minus_csat_dense_rank <= 5
      AND first_known_requester_id_minus_csat <= 18
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        NTH_VALUE(running_count, 2) OVER (PARTITION BY requester_id_dense_rank, distinct_agent_team_in_agent_id_channel ORDER BY first_known_requester_id_minus_csat_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_running_count,
        ROUND(requester_id_change / NULLIF(SUM(requester_id_change) OVER (PARTITION BY distinct_agent_team_in_agent_id_channel, closed_at_rank), 0), 4) AS requester_id_change_share_of_distinct_agent_team,
        LAST_VALUE(requester_id_change) IGNORE NULLS OVER (PARTITION BY requester_id_dense_rank, distinct_agent_team_in_agent_id_channel ORDER BY first_known_requester_id_minus_csat_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_requester_id_change,
        ROUND(requester_id / NULLIF(SUM(requester_id) OVER (PARTITION BY distinct_agent_team_in_agent_id_channel, closed_at_rank), 0), 4) AS requester_id_share_of_distinct_agent_team_in_age,
        RANK() OVER (PARTITION BY agent_team ORDER BY running_count DESC, id) AS running_count_rank,
        DENSE_RANK() OVER w_agent_team_by_running_count_2 AS running_count_dense_rank
    FROM top_per_group
    WINDOW w_agent_team_by_running_count_2 AS (PARTITION BY agent_team ORDER BY running_count DESC)
)
SELECT
    id,
    requester_id,
    channel,
    tags,
    requester_id_minus_csat,
    agent_name,
    agent_team,
    requester_id_minus_csat_change,
    requester_id_dense_rank,
    requester_id_minus_csat_share_of_closed_at_year_,
    closed_at_rank,
    first_known_requester_id_minus_csat,
    distinct_agent_team_in_agent_id_channel,
    requester_id_change,
    running_count,
    prev3_requester_id,
    first_known_requester_id_minus_csat_2,
    agent_id_channel_sum_requester_id_minus_csat,
    second_running_count,
    requester_id_change_share_of_distinct_agent_team,
    running_count_rank,
    running_count_dense_rank,
    DATE_TRUNC('YEAR', closed_at) AS closed_at_year
FROM ranked
WHERE distinct_agent_team_in_agent_id_channel <= 7
ORDER BY agent_team, requester_id_dense_rank DESC
