WITH cleaned_tickets AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        priority,
        channel,
        csat,
        tags
    FROM tickets
    WHERE agent_id IS NOT NULL
      AND csat < 4
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        SUM(requester_id) OVER (PARTITION BY agent_id, YEAR(closed_at) ORDER BY requester_id DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS requester_id_sum_2,
        LAST_VALUE(closed_at) OVER (ORDER BY closed_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_closed_at,
        ROUND(AVG(csat) OVER (PARTITION BY agent_id, YEAR(closed_at) ORDER BY requester_id DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS csat_avg_3_prior,
        ROUND(requester_id / NULLIF(SUM(requester_id) OVER (PARTITION BY agent_id, priority), 0), 4) AS requester_id_share_of_agent_id_priority,
        ROUND(CUME_DIST() OVER (PARTITION BY channel, csat ORDER BY requester_id, id), 3) AS requester_id_cume_dist,
        ROUND(AVG(csat) OVER w_agent_id_priority, 2) AS agent_id_priority_avg_csat,
        ROW_NUMBER() OVER w_agent_id_by_closed_at AS seq_no,
        MIN(requester_id) OVER (PARTITION BY agent_id, priority) AS agent_id_priority_min_requester_id,
        SUM(csat) OVER (PARTITION BY channel, csat ORDER BY requester_id, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS csat_sum_2_prior,
        ROUND(requester_id - AVG(requester_id) OVER w_agent_id_channel, 2) AS requester_id_vs_agent_id_channel_avg,
        NTH_VALUE(requester_id, 2) OVER (PARTITION BY channel, csat ORDER BY requester_id, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_requester_id,
        SIZE(COLLECT_SET(channel) OVER (PARTITION BY agent_id, priority)) AS distinct_channel_in_agent_id_priority,
        ROUND(AVG(csat) OVER (PARTITION BY agent_id, channel), 2) AS agent_id_channel_avg_csat
    FROM cleaned_tickets
    WINDOW w_agent_id_priority AS (PARTITION BY agent_id, priority), w_agent_id_by_closed_at AS (PARTITION BY agent_id ORDER BY closed_at DESC, id), w_agent_id_channel AS (PARTITION BY agent_id, channel)
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        closed_at,
        priority,
        csat,
        requester_id_sum_2,
        last_closed_at,
        csat_avg_3_prior,
        requester_id_cume_dist,
        agent_id_priority_avg_csat,
        agent_id_priority_min_requester_id,
        csat_sum_2_prior,
        second_requester_id,
        distinct_channel_in_agent_id_priority,
        agent_id_channel_avg_csat,
        MIN(csat_avg_3_prior) OVER (PARTITION BY seq_no, YEAR(last_closed_at)) AS seq_no_last_closed_at_year_min_csat_avg_3_prior,
        ROUND(PERCENT_RANK() OVER w_all_by_requester_id_ties, 3) AS requester_id_percent_rank,
        ROUND(csat - AVG(csat) OVER (PARTITION BY seq_no, YEAR(last_closed_at)), 2) AS csat_vs_seq_no_last_closed_at_year_avg,
        ROW_NUMBER() OVER (PARTITION BY seq_no, agent_id ORDER BY requester_id_share_of_agent_id_priority, id) AS seq_no_2
    FROM metrics
    WINDOW w_all_by_requester_id_ties AS (ORDER BY requester_id)
)
-- Final projection
SELECT
    id,
    priority,
    csat,
    requester_id_sum_2,
    last_closed_at,
    csat_avg_3_prior,
    requester_id_cume_dist,
    agent_id_priority_avg_csat,
    agent_id_priority_min_requester_id,
    csat_sum_2_prior,
    second_requester_id,
    distinct_channel_in_agent_id_priority,
    agent_id_channel_avg_csat,
    seq_no_last_closed_at_year_min_csat_avg_3_prior,
    requester_id_percent_rank,
    csat_vs_seq_no_last_closed_at_year_avg,
    seq_no_2,
    TO_DATE(closed_at) AS closed_at_date,
    LENGTH(TRIM(priority)) AS priority_len
FROM with_windows
ORDER BY priority, seq_no_last_closed_at_year_min_csat_avg_3_prior NULLS LAST
