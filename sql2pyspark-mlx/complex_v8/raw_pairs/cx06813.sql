WITH filtered AS (
    -- Scope the tickets rows
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        channel
    FROM tickets
    WHERE csat >= 2
      AND HOUR(opened_at) BETWEEN 10 AND 16
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        FIRST_VALUE(closed_at) OVER (PARTITION BY channel ORDER BY closed_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_closed_at,
        LAG(opened_at) OVER (ORDER BY closed_at DESC, id) AS prev_opened_at,
        ROUND((UNIX_TIMESTAMP(closed_at) - UNIX_TIMESTAMP(LAG(closed_at) OVER w_channel_by_closed_at)) / 60, 1) AS minutes_since_prev_closed_at,
        DENSE_RANK() OVER (ORDER BY closed_at) AS closed_at_dense_rank,
        LAG(closed_at) OVER (PARTITION BY channel ORDER BY closed_at DESC, id) AS prev_closed_at,
        ROUND((UNIX_TIMESTAMP(closed_at) - UNIX_TIMESTAMP(LAG(closed_at) OVER (ORDER BY closed_at, id))) / 60, 1) AS minutes_since_prev_closed_at_2,
        LAST_VALUE(opened_at) IGNORE NULLS OVER (PARTITION BY channel ORDER BY closed_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_opened_at,
        DENSE_RANK() OVER w_all_by_opened_at_ties AS opened_at_dense_rank,
        ROUND(PERCENT_RANK() OVER w_channel_by_closed_at_2, 3) AS closed_at_percent_rank,
        LAST_VALUE(opened_at) OVER (PARTITION BY channel ORDER BY closed_at DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_opened_at,
        DENSE_RANK() OVER (PARTITION BY channel ORDER BY closed_at DESC, id) AS closed_at_dense_rank_2,
        LAG(opened_at, 2) OVER (ORDER BY closed_at DESC, id) AS prev2_opened_at,
        FIRST_VALUE(opened_at) OVER (ORDER BY closed_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_opened_at
    FROM filtered
    WINDOW w_channel_by_closed_at AS (PARTITION BY channel ORDER BY closed_at DESC, id), w_all_by_opened_at_ties AS (ORDER BY opened_at NULLS LAST), w_channel_by_closed_at_2 AS (PARTITION BY channel ORDER BY closed_at DESC NULLS FIRST, id)
),
leaders AS (
    -- First row per group
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        channel,
        first_closed_at,
        prev_opened_at,
        minutes_since_prev_closed_at,
        closed_at_dense_rank,
        prev_closed_at,
        minutes_since_prev_closed_at_2,
        last_known_opened_at,
        closed_at_percent_rank,
        last_opened_at,
        closed_at_dense_rank_2,
        prev2_opened_at,
        first_opened_at
    FROM ranked
    WHERE opened_at_dense_rank = 1
)
-- Final projection
SELECT
    id,
    opened_at,
    closed_at,
    first_closed_at,
    prev_opened_at,
    minutes_since_prev_closed_at,
    prev_closed_at,
    minutes_since_prev_closed_at_2,
    last_known_opened_at,
    closed_at_percent_rank,
    last_opened_at,
    closed_at_dense_rank_2,
    prev2_opened_at,
    first_opened_at,
    closed_at_dense_rank - closed_at_percent_rank AS closed_at_dense_rank_minus_closed_at_percent_ran
FROM leaders
WHERE MONTH(prev2_opened_at) IN (2, 3)
ORDER BY closed_at DESC, first_opened_at
