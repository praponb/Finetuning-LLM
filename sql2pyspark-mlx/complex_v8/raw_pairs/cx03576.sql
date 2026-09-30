WITH prepared_tickets AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        channel,
        csat,
        tags,
        csat - requester_id AS csat_minus_requester_id,
        CONCAT_WS(' / ', channel, priority) AS channel_priority_label
    FROM tickets
    WHERE priority = 'P1'
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        id,
        requester_id,
        channel,
        csat,
        tags,
        channel_priority_label,
        NTILE(4) OVER w_agent_id_csat_by_csat_minus AS csat_minus_requester_id_quartile,
        COUNT(*) OVER (PARTITION BY agent_id, channel) AS rows_in_agent_id_channel,
        MIN(requester_id) OVER w_agent_id_channel AS agent_id_channel_min_requester_id,
        csat - LAG(csat) OVER w_all_by_requester_id AS csat_change,
        MAX(csat_minus_requester_id) OVER (PARTITION BY channel, agent_id) AS channel_agent_id_max_csat_minus_requester_id,
        MIN(csat) OVER (PARTITION BY agent_id, channel) AS agent_id_channel_min_csat,
        FIRST_VALUE(requester_id) IGNORE NULLS OVER (PARTITION BY agent_id, csat ORDER BY csat_minus_requester_id DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_requester_id,
        requester_id - LAG(requester_id) OVER (PARTITION BY csat, channel ORDER BY requester_id, id) AS requester_id_change,
        ROUND(CUME_DIST() OVER (PARTITION BY csat ORDER BY csat ASC), 3) AS csat_cume_dist,
        SUM(csat_minus_requester_id) OVER (PARTITION BY csat, channel) AS csat_channel_sum_csat_minus_requester_id,
        ROUND(CUME_DIST() OVER (PARTITION BY agent_id, csat ORDER BY csat_minus_requester_id DESC), 3) AS csat_minus_requester_id_cume_dist,
        SUM(csat) OVER (PARTITION BY agent_id, channel) AS agent_id_channel_sum_csat,
        ROUND(PERCENT_RANK() OVER (PARTITION BY agent_id, csat ORDER BY csat_minus_requester_id DESC), 3) AS csat_minus_requester_id_percent_rank,
        MAX(csat) OVER w_channel_agent_id AS channel_agent_id_max_csat,
        DENSE_RANK() OVER w_csat_channel_by_requester_id AS requester_id_dense_rank,
        requester_id - LAG(requester_id) OVER (PARTITION BY csat ORDER BY csat ASC, id) AS requester_id_change_2
    FROM prepared_tickets
    WINDOW w_agent_id_csat_by_csat_minus AS (PARTITION BY agent_id, csat ORDER BY csat_minus_requester_id DESC, id), w_agent_id_channel AS (PARTITION BY agent_id, channel), w_all_by_requester_id AS (ORDER BY requester_id NULLS LAST, id), w_channel_agent_id AS (PARTITION BY channel, agent_id), w_csat_channel_by_requester_id AS (PARTITION BY csat, channel ORDER BY requester_id, id)
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        id,
        csat,
        channel_priority_label,
        csat_minus_requester_id_quartile,
        rows_in_agent_id_channel,
        agent_id_channel_min_requester_id,
        csat_change,
        agent_id_channel_min_csat,
        requester_id_change,
        csat_channel_sum_csat_minus_requester_id,
        csat_minus_requester_id_cume_dist,
        agent_id_channel_sum_csat,
        csat_minus_requester_id_percent_rank,
        requester_id_dense_rank,
        LAG(agent_id_channel_min_csat, 3) OVER (PARTITION BY csat ORDER BY csat_minus_requester_id_quartile NULLS LAST, id) AS prev3_agent_id_channel_min_csat,
        FIRST_VALUE(agent_id_channel_min_csat) IGNORE NULLS OVER (PARTITION BY csat ORDER BY csat_minus_requester_id_quartile NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_agent_id_channel_min_csat,
        NTILE(3) OVER (PARTITION BY csat ORDER BY csat_minus_requester_id_quartile DESC NULLS FIRST, id) AS csat_minus_requester_id_quartile_tercile,
        MIN(agent_id_channel_min_csat) OVER (PARTITION BY csat ORDER BY csat_minus_requester_id_quartile NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_agent_id_channel_min_csat_so_far,
        ROUND(csat_change / NULLIF(SUM(csat_change) OVER w_csat_minus_requester_id_quar, 0), 4) AS csat_change_share_of_csat_minus_requester_id_qua,
        FIRST_VALUE(requester_id_change) IGNORE NULLS OVER (PARTITION BY csat ORDER BY csat_minus_requester_id_quartile NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_requester_id_change,
        MIN(requester_id_change) OVER (PARTITION BY csat_minus_requester_id_quartile) AS csat_minus_requester_id_quartile_min_requester_i
    FROM ranked
    WINDOW w_csat_minus_requester_id_quar AS (PARTITION BY csat_minus_requester_id_quartile)
)
SELECT
    id,
    channel_priority_label,
    csat_minus_requester_id_quartile,
    agent_id_channel_min_requester_id,
    csat_change,
    agent_id_channel_min_csat,
    requester_id_change,
    csat_channel_sum_csat_minus_requester_id,
    csat_minus_requester_id_cume_dist,
    csat_minus_requester_id_percent_rank,
    prev3_agent_id_channel_min_csat,
    min_agent_id_channel_min_csat_so_far,
    csat_change_share_of_csat_minus_requester_id_qua,
    csat_minus_requester_id_quartile_min_requester_i
FROM sequenced
ORDER BY csat_change_share_of_csat_minus_requester_id_qua, id
LIMIT 5
