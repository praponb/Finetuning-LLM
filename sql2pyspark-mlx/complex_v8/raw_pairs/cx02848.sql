WITH recent_tickets AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        csat,
        tags
    FROM tickets
    WHERE closed_at < TIMESTAMP '2025-03-08 23:00:00'
      AND opened_at >= TIMESTAMP '2025-01-13 11:00:00'
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        agent_id,
        opened_at,
        priority,
        ROUND(AVG(requester_id) OVER (PARTITION BY csat, priority ORDER BY csat, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS requester_id_avg_3,
        DENSE_RANK() OVER (PARTITION BY TO_DATE(opened_at), YEAR(closed_at) ORDER BY opened_at) AS opened_at_dense_rank,
        SIZE(COLLECT_SET(priority) OVER (PARTITION BY csat)) AS distinct_priority_in_csat,
        COUNT(*) OVER (PARTITION BY csat) AS rows_in_csat,
        SUM(csat) OVER (ORDER BY closed_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_csat,
        LEAD(closed_at) OVER (PARTITION BY TO_DATE(opened_at), YEAR(closed_at) ORDER BY opened_at, id) AS next_closed_at,
        FIRST_VALUE(csat) OVER (PARTITION BY TO_DATE(opened_at), YEAR(closed_at) ORDER BY opened_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_csat,
        ROUND(requester_id / NULLIF(SUM(requester_id) OVER w_channel, 0), 4) AS requester_id_share_of_channel,
        MIN(requester_id) OVER (PARTITION BY agent_id, channel ORDER BY closed_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_requester_id_so_far,
        ROUND(csat - AVG(csat) OVER (PARTITION BY csat), 2) AS csat_vs_csat_avg,
        ROW_NUMBER() OVER (PARTITION BY agent_id, channel ORDER BY closed_at DESC, id) AS seq_no,
        ROW_NUMBER() OVER (ORDER BY opened_at DESC, id) AS seq_no_2,
        LAG(opened_at) OVER (PARTITION BY agent_id, channel ORDER BY closed_at DESC, id) AS prev_opened_at
    FROM recent_tickets
    WINDOW w_channel AS (PARTITION BY channel)
),
leaders AS (
    -- First row per group
    SELECT *
    FROM sequenced
    WHERE opened_at_dense_rank = 1
      AND distinct_priority_in_csat > 4
),
per_group AS (
    -- Group-level totals
    SELECT
        first_csat,
        agent_id,
        COUNT(DISTINCT id) AS distinct_ids,
        MIN(min_requester_id_so_far) AS min_min_requester_id_so_far,
        COUNT(*) AS n_rows,
        SUM(requester_id_share_of_channel) AS total_requester_id_share_of_channel
    FROM leaders
    GROUP BY 1, 2
),
sequenced_2 AS (
    -- Running and moving aggregates
    SELECT
        first_csat,
        agent_id,
        distinct_ids,
        min_min_requester_id_so_far,
        SUM(n_rows) OVER (ORDER BY total_requester_id_share_of_channel NULLS LAST, first_csat, agent_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_n_rows,
        LAG(n_rows) OVER (ORDER BY total_requester_id_share_of_channel NULLS LAST, first_csat, agent_id) AS prev_n_rows,
        NTH_VALUE(n_rows, 2) OVER (ORDER BY total_requester_id_share_of_channel NULLS LAST, first_csat, agent_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_n_rows,
        ROUND(min_min_requester_id_so_far - AVG(min_min_requester_id_so_far) OVER (PARTITION BY distinct_ids, first_csat), 2) AS min_min_requester_id_so_far_vs_distinct_ids_firs
    FROM per_group
)
SELECT
    first_csat,
    distinct_ids,
    running_n_rows,
    prev_n_rows,
    second_n_rows,
    FLOOR(second_n_rows / 2) * 2 AS second_n_rows_bucket,
    FLOOR(second_n_rows / 2) * 2 AS second_n_rows_bucket_2
FROM sequenced_2
ORDER BY second_n_rows_bucket_2, prev_n_rows DESC
