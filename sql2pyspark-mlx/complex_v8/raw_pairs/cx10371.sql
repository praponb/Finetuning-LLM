WITH prepared_tickets AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        priority,
        channel,
        csat,
        tags,
        ROUND(csat / NULLIF(requester_id, 0), 3) AS csat_per_requester_id
    FROM tickets
    WHERE agent_id IS NOT NULL
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        RANK() OVER w_csat_agent_id_by_csat AS csat_rank,
        SUM(requester_id) OVER (PARTITION BY priority) AS priority_sum_requester_id,
        SUM(csat_per_requester_id) OVER (PARTITION BY MONTH(opened_at), priority ORDER BY requester_id DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_csat_per_requester_id,
        COUNT(*) OVER (PARTITION BY channel) AS rows_in_channel,
        ROUND(CUME_DIST() OVER (PARTITION BY channel ORDER BY opened_at DESC, id), 3) AS opened_at_cume_dist,
        LEAD(requester_id) OVER (PARTITION BY csat, agent_id ORDER BY csat, id) AS next_requester_id,
        LEAD(csat_per_requester_id, 3) OVER (PARTITION BY csat, agent_id ORDER BY csat, id) AS next3_csat_per_requester_id,
        NTH_VALUE(csat, 2) OVER (PARTITION BY csat, agent_id ORDER BY csat, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_csat
    FROM prepared_tickets
    WINDOW w_csat_agent_id_by_csat AS (PARTITION BY csat, agent_id ORDER BY csat, id)
),
with_trends_2 AS (
    -- Previous / next values per key
    SELECT
        *,
        SUM(priority_sum_requester_id) OVER (ORDER BY next_requester_id ASC NULLS LAST, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS priority_sum_requester_id_sum_2_prior,
        ROUND(rows_in_channel - AVG(rows_in_channel) OVER (PARTITION BY channel), 2) AS rows_in_channel_vs_channel_avg,
        NTH_VALUE(next3_csat_per_requester_id, 2) OVER (PARTITION BY csat_rank ORDER BY rows_in_channel DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_next3_csat_per_requester_id,
        LAG(next3_csat_per_requester_id, 3) OVER (PARTITION BY csat_rank ORDER BY rows_in_channel DESC, id) AS prev3_next3_csat_per_requester_id,
        ROUND(CUME_DIST() OVER (PARTITION BY csat_rank ORDER BY rows_in_channel DESC, id), 3) AS rows_in_channel_cume_dist,
        LEAD(priority_sum_requester_id, 2) OVER w_csat_rank_by_rows_in_channel_2 AS next2_priority_sum_requester_id
    FROM with_trends
    WINDOW w_csat_rank_by_rows_in_channel_2 AS (PARTITION BY csat_rank ORDER BY rows_in_channel DESC, id)
)
-- Final projection
SELECT
    id,
    requester_id,
    agent_id,
    opened_at,
    priority,
    channel,
    csat,
    csat_per_requester_id,
    priority_sum_requester_id,
    running_csat_per_requester_id,
    rows_in_channel,
    opened_at_cume_dist,
    next_requester_id,
    next3_csat_per_requester_id,
    second_csat,
    second_next3_csat_per_requester_id,
    prev3_next3_csat_per_requester_id,
    rows_in_channel_cume_dist,
    QUARTER(opened_at) AS opened_at_quarter
FROM with_trends_2
