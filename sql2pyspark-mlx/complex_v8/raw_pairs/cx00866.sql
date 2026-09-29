WITH scoped AS (
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
        LOWER(REGEXP_REPLACE(TRIM(priority), ' ', '_')) AS priority_slug
    FROM tickets
    WHERE (opened_at >= TIMESTAMP '2025-02-06 11:00:00' OR requester_id <= 21)
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        RANK() OVER (PARTITION BY agent_id ORDER BY closed_at ASC, id) AS closed_at_rank,
        DENSE_RANK() OVER w_agent_id_by_closed_at_ties AS closed_at_dense_rank,
        COUNT(*) OVER w_csat AS rows_in_csat,
        DENSE_RANK() OVER w_csat_by_closed_at AS closed_at_dense_rank_2,
        MIN(csat) OVER (PARTITION BY csat) AS csat_min_csat,
        ROUND(CUME_DIST() OVER w_csat_by_closed_at, 3) AS closed_at_cume_dist,
        ROW_NUMBER() OVER (PARTITION BY csat ORDER BY closed_at ASC, id) AS seq_no,
        LEAD(closed_at) OVER (PARTITION BY csat ORDER BY closed_at ASC, id) AS next_closed_at
    FROM scoped
    WINDOW w_agent_id_by_closed_at_ties AS (PARTITION BY agent_id ORDER BY closed_at ASC), w_csat AS (PARTITION BY csat), w_csat_by_closed_at AS (PARTITION BY csat ORDER BY closed_at ASC, id)
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER w_agent_id_by_closed_at_dense, 3) AS closed_at_dense_rank_percent_rank,
        ROUND(rows_in_csat / NULLIF(SUM(rows_in_csat) OVER (PARTITION BY closed_at_dense_rank_2), 0), 4) AS rows_in_csat_share_of_closed_at_dense_rank_2,
        ROW_NUMBER() OVER w_agent_id_by_next_closed_at AS seq_no_2,
        FIRST_VALUE(closed_at) OVER (PARTITION BY agent_id ORDER BY next_closed_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_closed_at,
        FIRST_VALUE(csat) IGNORE NULLS OVER (ORDER BY next_closed_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_csat,
        FIRST_VALUE(closed_at_rank) OVER (PARTITION BY agent_id ORDER BY next_closed_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_closed_at_rank
    FROM sequenced
    WINDOW w_agent_id_by_closed_at_dense AS (PARTITION BY agent_id ORDER BY closed_at_dense_rank ASC), w_agent_id_by_next_closed_at AS (PARTITION BY agent_id ORDER BY next_closed_at DESC, id)
)
-- Final projection
SELECT
    id,
    agent_id,
    opened_at,
    priority,
    channel,
    csat,
    tags,
    priority_slug,
    closed_at_dense_rank,
    rows_in_csat,
    closed_at_cume_dist,
    seq_no,
    next_closed_at,
    closed_at_dense_rank_percent_rank,
    first_closed_at,
    first_closed_at_rank,
    CONCAT(channel, '#', CAST(id AS STRING)) AS channel_ref,
    CASE WHEN csat >= 4 THEN 'A' WHEN csat >= 3 THEN 'B' ELSE 'C' END AS csat_tier
FROM with_trends
WHERE closed_at_rank <= 9
ORDER BY csat NULLS LAST
