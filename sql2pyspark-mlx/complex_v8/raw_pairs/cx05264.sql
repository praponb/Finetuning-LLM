WITH cleaned_tickets AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        csat,
        ABS(requester_id - 13) AS requester_id_distance
    FROM tickets
    WHERE MONTH(opened_at) IN (1, 2)
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(AVG(requester_id_distance) OVER (PARTITION BY TO_DATE(closed_at)), 2) AS closed_at_to_date_avg_requester_id_distance,
        ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC('MONTH', closed_at) ORDER BY closed_at NULLS LAST, id) AS seq_no,
        SUM(requester_id_distance) OVER (PARTITION BY agent_id, csat ORDER BY requester_id, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_requester_id_distance,
        FIRST_VALUE(opened_at) IGNORE NULLS OVER (PARTITION BY DATE_TRUNC('MONTH', closed_at) ORDER BY csat, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_opened_at,
        MAX(requester_id_distance) OVER (PARTITION BY TO_DATE(closed_at)) AS closed_at_to_date_max_requester_id_distance,
        LAST_VALUE(closed_at) IGNORE NULLS OVER (PARTITION BY agent_id, csat ORDER BY requester_id, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_closed_at,
        NTILE(10) OVER (PARTITION BY DATE_TRUNC('MONTH', closed_at) ORDER BY closed_at NULLS LAST, id) AS closed_at_decile,
        FIRST_VALUE(opened_at) OVER (PARTITION BY agent_id, csat ORDER BY requester_id, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_opened_at,
        ROUND(requester_id_distance - AVG(requester_id_distance) OVER (PARTITION BY TO_DATE(closed_at)), 2) AS requester_id_distance_vs_closed_at_to_date_avg,
        ROUND(requester_id - AVG(requester_id) OVER (PARTITION BY TO_DATE(closed_at)), 2) AS requester_id_vs_closed_at_to_date_avg,
        ROUND(requester_id / NULLIF(SUM(requester_id) OVER (PARTITION BY TO_DATE(closed_at)), 0), 4) AS requester_id_share_of_closed_at_to_date,
        COUNT(*) OVER (PARTITION BY TO_DATE(closed_at)) AS rows_in_closed_at_to_date,
        ROUND(AVG(requester_id) OVER (PARTITION BY TO_DATE(closed_at)), 2) AS closed_at_to_date_avg_requester_id
    FROM cleaned_tickets
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        agent_id,
        opened_at,
        priority,
        channel,
        csat,
        requester_id_distance,
        seq_no,
        running_requester_id_distance,
        requester_id_distance_vs_closed_at_to_date_avg,
        requester_id_share_of_closed_at_to_date,
        rows_in_closed_at_to_date,
        closed_at_to_date_avg_requester_id,
        SUM(rows_in_closed_at_to_date) OVER (PARTITION BY csat) AS csat_sum_rows_in_closed_at_to_date,
        ROUND((UNIX_TIMESTAMP(first_opened_at) - UNIX_TIMESTAMP(LAG(first_opened_at) OVER (ORDER BY requester_id_distance ASC, id))) / 60, 1) AS minutes_since_prev_first_opened_at,
        SIZE(COLLECT_SET(agent_id) OVER w_csat) AS distinct_agent_id_in_csat,
        MAX(seq_no) OVER (PARTITION BY TO_DATE(first_opened_at) ORDER BY first_opened_at ASC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS seq_no_max_2,
        LEAD(requester_id, 1, 0) OVER (PARTITION BY csat, closed_at_decile ORDER BY csat DESC, id) AS next_requester_id,
        DENSE_RANK() OVER (PARTITION BY csat, closed_at_decile ORDER BY csat DESC) AS csat_dense_rank
    FROM sequenced
    WINDOW w_csat AS (PARTITION BY csat)
)
SELECT
    id,
    agent_id,
    opened_at,
    priority,
    csat,
    requester_id_distance,
    seq_no,
    running_requester_id_distance,
    requester_id_distance_vs_closed_at_to_date_avg,
    requester_id_share_of_closed_at_to_date,
    rows_in_closed_at_to_date,
    csat_dense_rank,
    COALESCE(channel, 'UNKNOWN') AS channel_or_unknown,
    SUBSTRING(TRIM(priority), 1, 2) AS priority_prefix
FROM with_trends
ORDER BY opened_at DESC, csat, id
LIMIT 20
