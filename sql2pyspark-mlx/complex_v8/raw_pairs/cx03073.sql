WITH cleaned_tickets AS (
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
        tags,
        ABS(csat - 4) AS csat_distance
    FROM tickets
    WHERE csat < 4
),
joined_agent AS (
    -- Attach agent attributes
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.closed_at,
        s.tags,
        s.csat_distance,
        su.team AS agent_team,
        su.agent_name,
        su.hired_on AS agent_hired_on
    FROM cleaned_tickets s
    JOIN support_agents su ON su.id = s.agent_id
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LEAD(csat_distance, 2, 0) OVER w_all_by_requester_id AS next2_csat_distance,
        NTH_VALUE(agent_hired_on, 2) OVER (PARTITION BY agent_team, csat_distance ORDER BY closed_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_agent_hired_on,
        DENSE_RANK() OVER (ORDER BY requester_id) AS requester_id_dense_rank,
        LAG(csat_distance, 1, 0) OVER w_all_by_requester_id AS prev_csat_distance,
        ROUND(requester_id - AVG(requester_id) OVER (PARTITION BY agent_team), 2) AS requester_id_vs_agent_team_avg,
        MIN(csat_distance) OVER w_agent_id_closed_at_to_date AS agent_id_closed_at_to_date_min_csat_distance,
        LAG(csat_distance, 1, 0) OVER (ORDER BY csat_distance DESC, id) AS prev_csat_distance_2,
        FIRST_VALUE(agent_hired_on) OVER (PARTITION BY agent_id, DATE_TRUNC('MONTH', agent_hired_on) ORDER BY closed_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_agent_hired_on,
        COUNT(*) OVER w_agent_team AS rows_in_agent_team,
        MAX(csat_distance) OVER (PARTITION BY agent_id, TO_DATE(closed_at)) AS agent_id_closed_at_to_date_max_csat_distance,
        ROUND(PERCENT_RANK() OVER (ORDER BY csat_distance DESC, id), 3) AS csat_distance_percent_rank,
        ROUND(PERCENT_RANK() OVER (ORDER BY requester_id, id), 3) AS requester_id_percent_rank
    FROM joined_agent
    WINDOW w_all_by_requester_id AS (ORDER BY requester_id, id), w_agent_id_closed_at_to_date AS (PARTITION BY agent_id, TO_DATE(closed_at)), w_agent_team AS (PARTITION BY agent_team)
),
sequenced_2 AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(next2_csat_distance - AVG(next2_csat_distance) OVER w_prev_csat_distance_prev_csat, 2) AS next2_csat_distance_vs_prev_csat_distance_prev_c,
        RANK() OVER (ORDER BY first_agent_hired_on ASC, id) AS first_agent_hired_on_rank,
        SUM(requester_id_percent_rank) OVER (ORDER BY first_agent_hired_on ASC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS requester_id_percent_rank_sum_7,
        SUM(csat_distance_percent_rank) OVER (PARTITION BY next2_csat_distance ORDER BY agent_hired_on ASC NULLS LAST, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS csat_distance_percent_rank_sum_4
    FROM sequenced
    WINDOW w_prev_csat_distance_prev_csat AS (PARTITION BY prev_csat_distance, prev_csat_distance_2)
)
SELECT
    id,
    requester_id,
    agent_id,
    tags,
    csat_distance,
    next2_csat_distance,
    second_agent_hired_on,
    requester_id_dense_rank,
    agent_id_closed_at_to_date_min_csat_distance,
    first_agent_hired_on,
    first_agent_hired_on_rank,
    requester_id_percent_rank_sum_7,
    csat_distance_percent_rank_sum_4
FROM sequenced_2
WHERE csat_distance_percent_rank_sum_4 BETWEEN 0.3 AND 2.5
ORDER BY requester_id, requester_id_percent_rank_sum_7 DESC NULLS FIRST
