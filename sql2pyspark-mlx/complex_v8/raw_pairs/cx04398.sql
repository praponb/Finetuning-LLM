WITH base AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        channel,
        DATE_TRUNC('YEAR', opened_at) AS opened_at_year
    FROM tickets
    WHERE requester_id >= 7
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        id,
        requester_id,
        closed_at,
        channel,
        opened_at_year,
        DENSE_RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', opened_at) ORDER BY opened_at, id) AS opened_at_dense_rank,
        NTH_VALUE(opened_at, 2) OVER (PARTITION BY agent_id ORDER BY opened_at_year ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_opened_at,
        ROUND(requester_id - AVG(requester_id) OVER (PARTITION BY agent_id), 2) AS requester_id_vs_agent_id_avg,
        ROUND(PERCENT_RANK() OVER w_all_by_closed_at_ties, 3) AS closed_at_percent_rank,
        SUM(requester_id) OVER (PARTITION BY agent_id ORDER BY opened_at DESC NULLS FIRST, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS requester_id_sum_2,
        ROUND((UNIX_TIMESTAMP(opened_at) - UNIX_TIMESTAMP(LAG(opened_at) OVER (PARTITION BY agent_id ORDER BY opened_at DESC NULLS FIRST, id))) / 60, 1) AS minutes_since_prev_opened_at,
        ROUND(AVG(requester_id) OVER (PARTITION BY agent_id), 2) AS agent_id_avg_requester_id,
        ROUND(requester_id / NULLIF(SUM(requester_id) OVER (PARTITION BY agent_id), 0), 4) AS requester_id_share_of_agent_id,
        ROUND(requester_id / NULLIF(SUM(requester_id) OVER (PARTITION BY agent_id, TO_DATE(opened_at)), 0), 4) AS requester_id_share_of_agent_id_opened_at_to_date,
        ROUND(AVG(requester_id) OVER (PARTITION BY TO_DATE(opened_at) ORDER BY opened_at ASC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS requester_id_avg_6_prior,
        ROUND((UNIX_TIMESTAMP(opened_at_year) - UNIX_TIMESTAMP(LAG(opened_at_year) OVER w_channel_agent_id_by_opened_a)) / 60, 1) AS minutes_since_prev_opened_at_year,
        MIN(requester_id) OVER (PARTITION BY channel, agent_id ORDER BY opened_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_requester_id_so_far,
        MAX(requester_id) OVER (PARTITION BY agent_id) AS agent_id_max_requester_id
    FROM base
    WINDOW w_all_by_closed_at_ties AS (ORDER BY closed_at), w_channel_agent_id_by_opened_a AS (PARTITION BY channel, agent_id ORDER BY opened_at DESC, id)
),
leaders AS (
    -- First row per group
    SELECT *
    FROM sequenced
    WHERE opened_at_dense_rank = 1
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        requester_id,
        channel,
        opened_at_year,
        opened_at_dense_rank,
        second_opened_at,
        requester_id_vs_agent_id_avg,
        closed_at_percent_rank,
        requester_id_sum_2,
        agent_id_avg_requester_id,
        requester_id_share_of_agent_id,
        requester_id_share_of_agent_id_opened_at_to_date,
        requester_id_avg_6_prior,
        minutes_since_prev_opened_at_year,
        min_requester_id_so_far,
        agent_id_max_requester_id,
        SIZE(COLLECT_SET(channel) OVER (PARTITION BY opened_at_dense_rank)) AS distinct_channel_in_opened_at_dense_rank,
        ROW_NUMBER() OVER (PARTITION BY YEAR(closed_at) ORDER BY second_opened_at DESC, id) AS second_opened_at_seq,
        LAST_VALUE(opened_at_dense_rank) IGNORE NULLS OVER (PARTITION BY YEAR(closed_at) ORDER BY second_opened_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_opened_at_dense_rank,
        ROUND(requester_id_sum_2 - AVG(requester_id_sum_2) OVER (PARTITION BY opened_at_dense_rank), 2) AS requester_id_sum_2_vs_opened_at_dense_rank_avg,
        SUM(requester_id_sum_2) OVER (PARTITION BY opened_at_dense_rank) AS opened_at_dense_rank_sum_requester_id_sum_2,
        DENSE_RANK() OVER (PARTITION BY YEAR(closed_at) ORDER BY second_opened_at DESC, id) AS second_opened_at_dense_rank,
        ROUND(requester_id_avg_6_prior - AVG(requester_id_avg_6_prior) OVER (PARTITION BY opened_at_dense_rank), 2) AS requester_id_avg_6_prior_vs_opened_at_dense_rank
    FROM leaders
)
SELECT
    id,
    requester_id,
    channel,
    opened_at_year,
    second_opened_at,
    requester_id_vs_agent_id_avg,
    closed_at_percent_rank,
    agent_id_avg_requester_id,
    requester_id_share_of_agent_id,
    requester_id_share_of_agent_id_opened_at_to_date,
    requester_id_avg_6_prior,
    min_requester_id_so_far,
    agent_id_max_requester_id,
    second_opened_at_seq,
    last_known_opened_at_dense_rank,
    opened_at_dense_rank_sum_requester_id_sum_2,
    second_opened_at_dense_rank,
    requester_id_avg_6_prior_vs_opened_at_dense_rank,
    DATE_TRUNC('QUARTER', second_opened_at) AS second_opened_at_quarter,
    MONTH(second_opened_at) AS second_opened_at_month
FROM with_trends
