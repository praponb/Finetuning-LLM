WITH scoped_tickets AS (
    -- Keep valid rows and derive helper columns
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
      AND csat BETWEEN 2 AND 4
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        requester_id,
        agent_id,
        priority,
        channel,
        csat,
        ROW_NUMBER() OVER (PARTITION BY priority, agent_id ORDER BY csat DESC, id) AS seq_no,
        SUM(csat) OVER (PARTITION BY priority, agent_id ORDER BY csat DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_csat,
        COUNT(*) OVER (PARTITION BY agent_id) AS rows_in_agent_id,
        ROUND(CUME_DIST() OVER (ORDER BY csat DESC, id), 3) AS csat_cume_dist,
        csat - LAG(csat) OVER (ORDER BY csat DESC, id) AS csat_change,
        ROUND(requester_id - AVG(requester_id) OVER (PARTITION BY channel), 2) AS requester_id_vs_channel_avg,
        LAST_VALUE(csat) OVER (PARTITION BY priority, agent_id ORDER BY csat DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_csat,
        LAG(closed_at, 3) OVER w_all_by_csat AS prev3_closed_at,
        DENSE_RANK() OVER w_all_by_csat_2 AS csat_dense_rank,
        FIRST_VALUE(closed_at) IGNORE NULLS OVER (PARTITION BY priority, agent_id ORDER BY csat DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_closed_at,
        ROUND(csat / NULLIF(SUM(csat) OVER w_agent_id, 0), 4) AS csat_share_of_agent_id,
        ROUND(PERCENT_RANK() OVER (ORDER BY csat DESC), 3) AS csat_percent_rank,
        ROUND(AVG(requester_id) OVER (ORDER BY csat DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS requester_id_avg_3
    FROM scoped_tickets
    WINDOW w_all_by_csat AS (ORDER BY csat DESC, id), w_all_by_csat_2 AS (ORDER BY csat ASC, id), w_agent_id AS (PARTITION BY agent_id)
),
top_rows AS (
    -- First row per group
    SELECT
        id,
        requester_id,
        agent_id,
        priority,
        channel,
        csat,
        seq_no,
        running_csat,
        rows_in_agent_id,
        csat_cume_dist,
        csat_change,
        requester_id_vs_channel_avg,
        last_csat,
        prev3_closed_at,
        first_known_closed_at,
        csat_share_of_agent_id,
        csat_percent_rank,
        requester_id_avg_3
    FROM with_trends
    WHERE csat_dense_rank = 1
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        csat_share_of_agent_id - LAG(csat_share_of_agent_id) OVER w_prev3_closed_at_to_date_by_p AS csat_share_of_agent_id_change,
        LEAD(requester_id_vs_channel_avg, 3) OVER (PARTITION BY priority ORDER BY csat_share_of_agent_id DESC, id) AS next3_requester_id_vs_channel_avg,
        MIN(seq_no) OVER (PARTITION BY TO_DATE(prev3_closed_at)) AS prev3_closed_at_to_date_min_seq_no,
        LAG(prev3_closed_at, 3) OVER w_prev3_closed_at_to_date_by_p AS prev3_prev3_closed_at,
        LAST_VALUE(csat_percent_rank) IGNORE NULLS OVER (PARTITION BY last_csat ORDER BY seq_no DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_csat_percent_rank
    FROM top_rows
    WINDOW w_prev3_closed_at_to_date_by_p AS (PARTITION BY TO_DATE(prev3_closed_at) ORDER BY prev3_closed_at ASC, id)
)
-- Final projection
SELECT
    id,
    agent_id,
    priority,
    channel,
    csat,
    seq_no,
    running_csat,
    rows_in_agent_id,
    csat_cume_dist,
    csat_change,
    last_csat,
    prev3_closed_at,
    first_known_closed_at,
    csat_share_of_agent_id,
    csat_percent_rank,
    requester_id_avg_3,
    csat_share_of_agent_id_change,
    next3_requester_id_vs_channel_avg,
    prev3_closed_at_to_date_min_seq_no,
    prev3_prev3_closed_at,
    last_known_csat_percent_rank
FROM with_windows
