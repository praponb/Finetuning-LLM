WITH base AS (
    -- Filter tickets
    SELECT *
    FROM tickets
    WHERE csat <= 4
      AND priority NOT IN ('P3', 'P4')
),
with_details AS (
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
        su.agent_name
    FROM base s
    LEFT JOIN support_agents su ON su.id = s.agent_id
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        agent_name,
        csat - LAG(csat) OVER (PARTITION BY csat ORDER BY csat ASC NULLS LAST, id) AS csat_change,
        LAST_VALUE(opened_at) OVER (PARTITION BY channel, csat ORDER BY closed_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_opened_at,
        LAG(closed_at) OVER (ORDER BY opened_at DESC NULLS FIRST, id) AS prev_closed_at,
        COUNT(*) OVER w_channel AS rows_in_channel,
        SIZE(COLLECT_SET(channel) OVER w_closed_at_to_date) AS distinct_channel_in_closed_at_to_date,
        MAX(csat) OVER (PARTITION BY agent_id ORDER BY requester_id ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_csat_so_far,
        SUM(requester_id) OVER (ORDER BY opened_at DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_requester_id,
        LAG(closed_at, 2) OVER (ORDER BY opened_at DESC NULLS FIRST, id) AS prev2_closed_at,
        SUM(requester_id) OVER w_closed_at_to_date AS closed_at_to_date_sum_requester_id,
        SUM(csat) OVER w_channel AS channel_sum_csat,
        ROUND(AVG(csat) OVER (PARTITION BY agent_id ORDER BY requester_id ASC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS csat_avg_6_prior,
        LAST_VALUE(csat) OVER (PARTITION BY csat ORDER BY csat ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_csat,
        MAX(requester_id) OVER (PARTITION BY channel) AS channel_max_requester_id,
        FIRST_VALUE(requester_id) OVER (PARTITION BY agent_id ORDER BY requester_id ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_requester_id,
        SIZE(COLLECT_SET(agent_id) OVER w_channel) AS distinct_agent_id_in_channel
    FROM with_details
    WINDOW w_channel AS (PARTITION BY channel), w_closed_at_to_date AS (PARTITION BY TO_DATE(closed_at))
),
scored AS (
    -- Add calculated columns
    SELECT *, COALESCE(csat_change, 0) AS csat_change_filled
    FROM windowed
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        id,
        requester_id,
        agent_id,
        agent_name,
        csat_change,
        last_opened_at,
        rows_in_channel,
        max_csat_so_far,
        running_requester_id,
        prev2_closed_at,
        closed_at_to_date_sum_requester_id,
        last_csat,
        channel_max_requester_id,
        first_requester_id,
        distinct_agent_id_in_channel,
        csat_change_filled,
        COUNT(*) OVER (PARTITION BY distinct_agent_id_in_channel) AS rows_in_distinct_agent_id_in_channel,
        LEAD(last_opened_at) OVER (PARTITION BY distinct_channel_in_closed_at_to_date, csat_change ORDER BY closed_at ASC, id) AS next_last_opened_at,
        SUM(first_requester_id) OVER w_distinct_agent_id_in_channel AS distinct_agent_id_in_channel_sum_first_requester,
        SIZE(COLLECT_SET(agent_id) OVER w_distinct_agent_id_in_channel) AS distinct_agent_id_in_distinct_agent_id_in_channe,
        ROUND(channel_max_requester_id / NULLIF(SUM(channel_max_requester_id) OVER (PARTITION BY distinct_agent_id_in_channel), 0), 4) AS channel_max_requester_id_share_of_distinct_agent,
        ROUND(csat_change / NULLIF(SUM(csat_change) OVER (PARTITION BY agent_id), 0), 4) AS csat_change_share_of_agent_id,
        NTH_VALUE(last_opened_at, 2) OVER (PARTITION BY distinct_channel_in_closed_at_to_date ORDER BY requester_id, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_last_opened_at
    FROM scored
    WINDOW w_distinct_agent_id_in_channel AS (PARTITION BY distinct_agent_id_in_channel)
)
SELECT
    id,
    requester_id,
    agent_name,
    csat_change,
    last_opened_at,
    rows_in_channel,
    max_csat_so_far,
    running_requester_id,
    closed_at_to_date_sum_requester_id,
    last_csat,
    channel_max_requester_id,
    distinct_agent_id_in_channel,
    csat_change_filled,
    distinct_agent_id_in_channel_sum_first_requester,
    distinct_agent_id_in_distinct_agent_id_in_channe,
    channel_max_requester_id_share_of_distinct_agent
FROM sequenced
ORDER BY requester_id DESC NULLS FIRST
