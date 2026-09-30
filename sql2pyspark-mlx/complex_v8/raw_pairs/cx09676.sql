WITH base_tickets AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        csat
    FROM tickets
    WHERE requester_id <= 23
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(AVG(csat) OVER (PARTITION BY channel, csat ORDER BY csat DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS csat_avg_1_prior,
        FIRST_VALUE(csat) OVER (PARTITION BY DATE_TRUNC('MONTH', closed_at), csat ORDER BY closed_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_csat,
        MIN(requester_id) OVER w_agent_id_channel AS agent_id_channel_min_requester_id,
        LEAD(closed_at) OVER (PARTITION BY DATE_TRUNC('MONTH', closed_at) ORDER BY opened_at DESC, id) AS next_closed_at,
        ROUND(AVG(requester_id) OVER (PARTITION BY agent_id, channel), 2) AS agent_id_channel_avg_requester_id,
        LAG(closed_at) OVER (PARTITION BY channel, csat ORDER BY csat DESC, id) AS prev_closed_at,
        RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', closed_at) ORDER BY opened_at DESC) AS opened_at_rank,
        MIN(csat) OVER (PARTITION BY agent_id, channel) AS agent_id_channel_min_csat,
        FIRST_VALUE(opened_at) OVER (PARTITION BY DATE_TRUNC('MONTH', closed_at) ORDER BY opened_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_opened_at,
        LAST_VALUE(opened_at) IGNORE NULLS OVER (PARTITION BY channel, csat ORDER BY csat DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_opened_at,
        ROUND(PERCENT_RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', closed_at), csat ORDER BY closed_at DESC), 3) AS closed_at_percent_rank,
        SIZE(COLLECT_SET(priority) OVER (PARTITION BY agent_id, channel)) AS distinct_priority_in_agent_id_channel
    FROM base_tickets
    WINDOW w_agent_id_channel AS (PARTITION BY agent_id, channel)
),
top_rows AS (
    -- Top 5 per group
    SELECT *
    FROM ranked
    WHERE opened_at_rank <= 5
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        MAX(agent_id_channel_min_csat) OVER (PARTITION BY opened_at_rank ORDER BY agent_id_channel_avg_requester_id, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS agent_id_channel_min_csat_max_4,
        FIRST_VALUE(agent_id_channel_min_csat) IGNORE NULLS OVER (PARTITION BY opened_at_rank ORDER BY agent_id_channel_avg_requester_id, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_agent_id_channel_min_csat,
        NTH_VALUE(csat, 2) OVER (PARTITION BY opened_at_rank ORDER BY agent_id_channel_avg_requester_id, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_csat,
        MAX(requester_id) OVER (PARTITION BY opened_at_rank, distinct_priority_in_agent_id_channel) AS opened_at_rank_distinct_priority_in_agent_id_cha,
        SUM(agent_id_channel_min_csat) OVER (PARTITION BY channel, agent_id ORDER BY closed_at_percent_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_agent_id_channel_min_csat
    FROM top_rows
)
SELECT
    id,
    requester_id,
    closed_at,
    channel,
    csat,
    csat_avg_1_prior,
    first_csat,
    agent_id_channel_min_requester_id,
    next_closed_at,
    agent_id_channel_avg_requester_id,
    prev_closed_at,
    opened_at_rank,
    agent_id_channel_min_csat,
    first_opened_at,
    last_known_opened_at,
    closed_at_percent_rank,
    distinct_priority_in_agent_id_channel,
    agent_id_channel_min_csat_max_4,
    first_known_agent_id_channel_min_csat,
    running_agent_id_channel_min_csat,
    TO_DATE(closed_at) AS closed_at_date
FROM with_windows
ORDER BY opened_at_rank DESC
