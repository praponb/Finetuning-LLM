WITH recent_tickets AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        csat,
        tags,
        closed_at + INTERVAL 2 HOURS AS closed_at_plus_2h,
        COALESCE(channel, 'UNKNOWN') AS channel_or_unknown
    FROM tickets
    WHERE channel = 'chat'
),
with_parent_agent AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        su.hired_on AS agent_hired_on,
        su.tier AS agent_tier,
        su.team AS agent_team
    FROM recent_tickets s
    LEFT JOIN support_agents su ON su.id = s.agent_id
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        SIZE(COLLECT_SET(agent_id) OVER w_channel_or_unknown) AS distinct_agent_id_in_channel_or_unknown,
        ROUND(CUME_DIST() OVER (PARTITION BY agent_team ORDER BY agent_hired_on, id), 3) AS agent_hired_on_cume_dist,
        ROW_NUMBER() OVER (PARTITION BY channel_or_unknown ORDER BY csat, id) AS seq_no,
        LAG(agent_hired_on) OVER (PARTITION BY csat, channel_or_unknown ORDER BY agent_hired_on ASC, id) AS prev_agent_hired_on,
        SIZE(COLLECT_SET(agent_id) OVER (PARTITION BY channel_or_unknown, YEAR(closed_at_plus_2h))) AS distinct_agent_id_in_channel_or_unknown_closed_a,
        ROUND(PERCENT_RANK() OVER (ORDER BY requester_id DESC), 3) AS requester_id_percent_rank,
        LAST_VALUE(agent_hired_on) IGNORE NULLS OVER (PARTITION BY agent_team ORDER BY agent_hired_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_agent_hired_on,
        SUM(requester_id) OVER w_channel_or_unknown AS channel_or_unknown_sum_requester_id,
        ROUND(AVG(requester_id) OVER (ORDER BY requester_id DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS requester_id_avg_1_prior,
        ROUND(csat - AVG(csat) OVER (PARTITION BY channel_or_unknown, YEAR(closed_at_plus_2h)), 2) AS csat_vs_channel_or_unknown_closed_at_plus_2h_yea,
        DENSE_RANK() OVER (ORDER BY requester_id ASC) AS requester_id_dense_rank,
        FIRST_VALUE(closed_at_plus_2h) IGNORE NULLS OVER (PARTITION BY agent_team ORDER BY agent_hired_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_closed_at_plus_2h,
        ROUND(requester_id - AVG(requester_id) OVER w_channel_or_unknown, 2) AS requester_id_vs_channel_or_unknown_avg,
        ROUND(AVG(csat) OVER (PARTITION BY csat, channel_or_unknown ORDER BY agent_hired_on ASC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS csat_avg_2
    FROM with_parent_agent
    WINDOW w_channel_or_unknown AS (PARTITION BY channel_or_unknown)
),
labeled AS (
    -- Label and score rows
    SELECT
        id,
        requester_id,
        agent_id,
        csat,
        tags,
        closed_at_plus_2h,
        channel_or_unknown,
        agent_hired_on,
        agent_tier,
        agent_team,
        seq_no,
        prev_agent_hired_on,
        distinct_agent_id_in_channel_or_unknown_closed_a,
        last_known_agent_hired_on,
        channel_or_unknown_sum_requester_id,
        requester_id_avg_1_prior,
        csat_vs_channel_or_unknown_closed_at_plus_2h_yea,
        requester_id_dense_rank,
        requester_id_vs_channel_or_unknown_avg,
        csat_avg_2,
        COALESCE(csat, 0) AS csat_filled,
        csat_avg_2 + requester_id_avg_1_prior AS csat_avg_2_plus_requester_id_avg_1_prior
    FROM windowed
    WHERE closed_at_plus_2h < TIMESTAMP '2025-02-05 09:00:00'
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY YEAR(prev_agent_hired_on), seq_no ORDER BY distinct_agent_id_in_channel_or_unknown_closed_a, id) AS distinct_agent_id_in_channel_or_unknown_closed_a_2,
        ROUND(CUME_DIST() OVER (PARTITION BY agent_id, YEAR(closed_at_plus_2h) ORDER BY requester_id_dense_rank DESC), 3) AS requester_id_dense_rank_cume_dist,
        NTILE(4) OVER w_agent_id_by_agent_hired_on AS agent_hired_on_quartile
    FROM labeled
    WINDOW w_agent_id_by_agent_hired_on AS (PARTITION BY agent_id ORDER BY agent_hired_on, id)
)
SELECT
    id,
    requester_id,
    agent_id,
    tags,
    closed_at_plus_2h,
    agent_team,
    seq_no,
    distinct_agent_id_in_channel_or_unknown_closed_a,
    last_known_agent_hired_on,
    channel_or_unknown_sum_requester_id,
    requester_id_avg_1_prior,
    requester_id_dense_rank,
    requester_id_vs_channel_or_unknown_avg,
    csat_avg_2,
    csat_filled,
    csat_avg_2_plus_requester_id_avg_1_prior,
    distinct_agent_id_in_channel_or_unknown_closed_a_2,
    requester_id_dense_rank_cume_dist,
    agent_hired_on_quartile,
    csat - requester_id_avg_1_prior AS csat_minus_requester_id_avg_1_prior
FROM metrics
ORDER BY channel_or_unknown_sum_requester_id DESC
