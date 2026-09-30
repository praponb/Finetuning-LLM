WITH base_policys AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        start_date,
        end_date,
        status
    FROM policies
    WHERE end_date >= DATE '2026-01-20'
),
with_details AS (
    -- Attach holder attributes
    SELECT
        s.id,
        s.holder_id,
        s.start_date,
        s.end_date,
        s.status,
        p.segment AS holder_segment,
        p.region AS holder_region
    FROM base_policys s
    JOIN policyholders p ON p.id = s.holder_id
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        DATEDIFF(start_date, LAG(start_date) OVER (PARTITION BY status, holder_region ORDER BY start_date DESC, id)) AS days_since_prev_start_date,
        DENSE_RANK() OVER (PARTITION BY status ORDER BY end_date DESC) AS end_date_dense_rank,
        FIRST_VALUE(end_date) IGNORE NULLS OVER (ORDER BY start_date NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_end_date,
        RANK() OVER (PARTITION BY holder_id, status ORDER BY end_date) AS end_date_rank,
        LAST_VALUE(start_date) OVER (ORDER BY start_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_start_date,
        FIRST_VALUE(start_date) OVER (PARTITION BY holder_segment ORDER BY start_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_start_date,
        RANK() OVER (ORDER BY start_date NULLS LAST, id) AS start_date_rank,
        ROUND(PERCENT_RANK() OVER (PARTITION BY status ORDER BY end_date DESC), 3) AS end_date_percent_rank,
        DENSE_RANK() OVER (ORDER BY start_date NULLS LAST, id) AS start_date_dense_rank,
        LAST_VALUE(start_date) IGNORE NULLS OVER (ORDER BY start_date NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_start_date
    FROM with_details
)
-- Final projection
SELECT
    id,
    holder_id,
    start_date,
    status,
    holder_segment,
    holder_region,
    last_start_date,
    first_start_date,
    start_date_rank,
    end_date_percent_rank,
    last_known_start_date,
    COALESCE(end_date_dense_rank, 0) AS end_date_dense_rank_filled,
    ABS(end_date_rank - 6) AS end_date_rank_distance
FROM sequenced
WHERE first_known_end_date IS NOT NULL
ORDER BY end_date_dense_rank_filled DESC
