WITH filtered AS (
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
        tags,
        QUARTER(closed_at) AS closed_at_quarter,
        ROUND(csat / NULLIF(requester_id, 0), 3) AS csat_per_requester_id
    FROM tickets
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(csat_per_requester_id) OVER (PARTITION BY agent_id, MONTH(opened_at) ORDER BY closed_at, id) AS prev_csat_per_requester_id,
        ROW_NUMBER() OVER (ORDER BY requester_id DESC, id) AS seq_no,
        LAST_VALUE(opened_at) OVER (ORDER BY requester_id DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_opened_at,
        ROUND(CUME_DIST() OVER (ORDER BY requester_id DESC), 3) AS requester_id_cume_dist
    FROM filtered
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        NTILE(5) OVER (PARTITION BY agent_id ORDER BY closed_at_quarter DESC, id) AS closed_at_quarter_quintile,
        DENSE_RANK() OVER (PARTITION BY csat ORDER BY csat_per_requester_id DESC NULLS FIRST, id) AS csat_per_requester_id_dense_rank,
        LAST_VALUE(prev_csat_per_requester_id) IGNORE NULLS OVER (PARTITION BY agent_id ORDER BY closed_at_quarter DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_prev_csat_per_requester_id,
        NTILE(4) OVER (PARTITION BY csat ORDER BY csat_per_requester_id DESC NULLS FIRST, id) AS csat_per_requester_id_quartile,
        SIZE(COLLECT_SET(channel) OVER w_priority) AS distinct_channel_in_priority,
        COUNT(*) OVER (PARTITION BY priority) AS rows_in_priority
    FROM windowed
    WINDOW w_priority AS (PARTITION BY priority)
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        priority,
        channel,
        csat,
        tags,
        closed_at_quarter,
        last_opened_at,
        requester_id_cume_dist,
        closed_at_quarter_quintile,
        csat_per_requester_id_dense_rank,
        last_known_prev_csat_per_requester_id,
        csat_per_requester_id_quartile,
        COUNT(*) OVER (PARTITION BY channel, csat ORDER BY seq_no DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        ROUND(csat_per_requester_id - AVG(csat_per_requester_id) OVER w_csat, 2) AS csat_per_requester_id_vs_csat_avg,
        ROUND(CUME_DIST() OVER w_channel_csat_by_seq_no_ties, 3) AS seq_no_cume_dist,
        SUM(csat) OVER (PARTITION BY channel, csat ORDER BY seq_no DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS csat_sum_3_prior,
        ROUND(AVG(distinct_channel_in_priority) OVER w_csat, 2) AS csat_avg_distinct_channel_in_priority,
        SIZE(COLLECT_SET(channel) OVER w_csat) AS distinct_channel_in_csat
    FROM ranked
    WINDOW w_csat AS (PARTITION BY csat), w_channel_csat_by_seq_no_ties AS (PARTITION BY channel, csat ORDER BY seq_no DESC)
)
-- Subtotals with CUBE
SELECT
    csat,
    csat_per_requester_id_dense_rank,
    GROUPING_ID() AS gid,
    COUNT(DISTINCT priority) AS distinct_prioritys,
    MIN(closed_at_quarter_quintile) AS min_closed_at_quarter_quintile,
    MIN(csat_avg_distinct_channel_in_priority) AS min_csat_avg_distinct_channel_in_priority
FROM sequenced
GROUP BY CUBE(csat, csat_per_requester_id_dense_rank)
ORDER BY csat NULLS LAST, csat_per_requester_id_dense_rank NULLS LAST
