WITH scoped_tickets AS (
    -- Scope the tickets rows
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        csat,
        tags
    FROM tickets
    WHERE priority NOT IN ('P2', 'P3')
      AND agent_id IS NOT NULL
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        RANK() OVER (ORDER BY closed_at ASC) AS closed_at_rank,
        DENSE_RANK() OVER (ORDER BY closed_at ASC, id) AS closed_at_dense_rank
    FROM scoped_tickets
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        MIN(closed_at_rank) OVER (PARTITION BY csat) AS csat_min_closed_at_rank,
        COUNT(*) OVER (PARTITION BY csat) AS rows_in_csat,
        LAST_VALUE(closed_at_rank) OVER (PARTITION BY agent_id ORDER BY closed_at DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_closed_at_rank,
        MAX(closed_at_dense_rank) OVER (PARTITION BY csat) AS csat_max_closed_at_dense_rank,
        NTH_VALUE(closed_at, 2) OVER (PARTITION BY agent_id, closed_at_rank ORDER BY closed_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_closed_at,
        LEAD(closed_at_rank, 1, 0) OVER (PARTITION BY closed_at_dense_rank, priority ORDER BY closed_at_dense_rank NULLS LAST, id) AS next_closed_at_rank
    FROM metrics
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, field, amount
FROM (
    SELECT id, closed_at_dense_rank, next_closed_at_rank, csat_min_closed_at_rank
    FROM with_windows
) src
UNPIVOT INCLUDE NULLS (
    amount FOR field IN (closed_at_dense_rank, next_closed_at_rank, csat_min_closed_at_rank)
) unp
