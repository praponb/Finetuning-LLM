WITH cleaned AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        priority,
        channel,
        csat,
        tags,
        CASE WHEN requester_id >= 17 THEN 'large' WHEN requester_id >= 7 THEN 'medium' ELSE 'small' END AS requester_id_tier
    FROM tickets
    WHERE requester_id <= 17
),
with_activity AS (
    -- Rows that have matching tickets
    SELECT s.*
    FROM cleaned s
    WHERE EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.agent_id = s.agent_id
              AND t.opened_at < TIMESTAMP '2025-03-08 11:00:00'
        )
      AND s.channel = 'chat'
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        DENSE_RANK() OVER w_priority_requester_id_tier_b AS closed_at_dense_rank,
        ROUND(PERCENT_RANK() OVER w_priority_requester_id_tier_b_2, 3) AS closed_at_percent_rank,
        DENSE_RANK() OVER (PARTITION BY priority, requester_id_tier ORDER BY closed_at DESC NULLS FIRST, id) AS closed_at_dense_rank_2,
        NTILE(4) OVER (PARTITION BY priority, requester_id_tier ORDER BY closed_at DESC NULLS FIRST, id) AS closed_at_quartile,
        SUM(csat) OVER (PARTITION BY priority, agent_id) AS priority_agent_id_sum_csat
    FROM with_activity
    WINDOW w_priority_requester_id_tier_b AS (PARTITION BY priority, requester_id_tier ORDER BY closed_at DESC NULLS FIRST), w_priority_requester_id_tier_b_2 AS (PARTITION BY priority, requester_id_tier ORDER BY closed_at DESC NULLS FIRST, id)
),
without_matches AS (
    -- Rows that have matching tickets
    SELECT s.*
    FROM with_windows s
    WHERE EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.agent_id = s.agent_id
              AND t.channel IN ('chat', 'web', 'email')
        )
),
grouped AS (
    -- Group-level totals
    SELECT
        priority,
        SUM(closed_at_dense_rank) AS total_closed_at_dense_rank,
        ROUND(AVG(closed_at_percent_rank), 2) AS avg_closed_at_percent_rank,
        COUNT(*) FILTER (WHERE channel IS NOT NULL) AS n_channel_is_not_null,
        COUNT(*) AS row_count
    FROM without_matches
    GROUP BY 1
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY priority ORDER BY row_count ASC NULLS LAST, priority) AS row_count_seq,
        ROUND(CUME_DIST() OVER (PARTITION BY priority ORDER BY row_count ASC NULLS LAST), 3) AS row_count_cume_dist,
        ROUND(avg_closed_at_percent_rank / NULLIF(SUM(avg_closed_at_percent_rank) OVER (PARTITION BY row_count, n_channel_is_not_null), 0), 4) AS avg_closed_at_percent_rank_share_of_row_count_n_
    FROM grouped
)
SELECT
    priority,
    total_closed_at_dense_rank,
    n_channel_is_not_null,
    row_count,
    avg_closed_at_percent_rank_share_of_row_count_n_
FROM ranked
ORDER BY n_channel_is_not_null DESC
