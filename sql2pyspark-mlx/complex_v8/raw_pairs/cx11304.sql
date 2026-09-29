WITH valid AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        priority,
        channel,
        tags,
        INITCAP(TRIM(priority)) AS priority_clean
    FROM tickets
    WHERE HOUR(opened_at) BETWEEN 10 AND 16
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        FIRST_VALUE(requester_id) OVER (ORDER BY requester_id, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_requester_id,
        LEAD(opened_at, 3) OVER (ORDER BY requester_id, id) AS next3_opened_at,
        SUM(requester_id) OVER (ORDER BY requester_id DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS requester_id_sum_6_prior,
        ROUND(CUME_DIST() OVER (ORDER BY requester_id), 3) AS requester_id_cume_dist,
        MAX(requester_id) OVER w_channel AS channel_max_requester_id,
        RANK() OVER w_all_by_requester_id_ties AS requester_id_rank,
        ROW_NUMBER() OVER (ORDER BY requester_id, id) AS seq_no,
        ROUND(CUME_DIST() OVER w_all_by_opened_at, 3) AS opened_at_cume_dist,
        LEAD(opened_at) OVER (ORDER BY opened_at, id) AS next_opened_at,
        ROW_NUMBER() OVER (ORDER BY opened_at NULLS LAST, id) AS opened_at_seq,
        LAST_VALUE(requester_id) OVER (ORDER BY opened_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_requester_id,
        requester_id - LAG(requester_id) OVER (ORDER BY requester_id DESC, id) AS requester_id_change,
        MAX(requester_id) OVER (ORDER BY requester_id DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_requester_id_so_far
    FROM valid
    WINDOW w_channel AS (PARTITION BY channel), w_all_by_requester_id_ties AS (ORDER BY requester_id), w_all_by_opened_at AS (ORDER BY opened_at NULLS LAST, id)
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY priority, requester_id_rank) AS rows_in_priority_requester_id_rank,
        LAG(requester_id_sum_6_prior, 2) OVER w_next_opened_at_year_by_seq_n AS prev2_requester_id_sum_6_prior,
        FIRST_VALUE(requester_id_change) IGNORE NULLS OVER (PARTITION BY MONTH(next3_opened_at), priority ORDER BY opened_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_requester_id_change,
        LAST_VALUE(opened_at) OVER (PARTITION BY MONTH(next3_opened_at), priority ORDER BY opened_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_opened_at,
        DENSE_RANK() OVER (PARTITION BY YEAR(next_opened_at) ORDER BY seq_no, id) AS seq_no_dense_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY YEAR(next_opened_at) ORDER BY seq_no, id), 3) AS seq_no_cume_dist,
        FIRST_VALUE(channel_max_requester_id) IGNORE NULLS OVER (PARTITION BY YEAR(next_opened_at) ORDER BY seq_no, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_channel_max_requester_id
    FROM with_windows
    WINDOW w_next_opened_at_year_by_seq_n AS (PARTITION BY YEAR(next_opened_at) ORDER BY seq_no, id)
)
-- Final projection
SELECT
    id,
    agent_id,
    opened_at,
    channel,
    tags,
    priority_clean,
    next3_opened_at,
    channel_max_requester_id,
    requester_id_rank,
    seq_no,
    opened_at_cume_dist,
    opened_at_seq,
    last_requester_id,
    requester_id_change,
    rows_in_priority_requester_id_rank,
    prev2_requester_id_sum_6_prior,
    first_known_requester_id_change,
    last_opened_at,
    seq_no_cume_dist,
    first_known_channel_max_requester_id
FROM metrics
ORDER BY opened_at_seq DESC
