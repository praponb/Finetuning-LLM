WITH filtered_policys AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        status
    FROM policies
    WHERE YEAR(start_date) = 2024
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        holder_id,
        status,
        COUNT(*) AS row_count,
        PERCENTILE_APPROX(premium, 0.5) AS median_premium
    FROM filtered_policys
    WHERE premium >= 2013.8
    GROUP BY holder_id, status
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(median_premium / NULLIF(SUM(median_premium) OVER w_status_holder_id, 0), 4) AS median_premium_share_of_status_holder_id,
        DENSE_RANK() OVER (ORDER BY row_count, holder_id, status) AS row_count_dense_rank
    FROM grouped
    WINDOW w_status_holder_id AS (PARTITION BY status, holder_id)
)
SELECT
    holder_id,
    status,
    row_count,
    median_premium_share_of_status_holder_id,
    LEAST(row_count, row_count_dense_rank) AS least_row_count_row_count_dense_rank,
    CASE WHEN status IN ('EXPIRED', 'LAPSED') THEN 'group_a' WHEN status = 'CANCELLED' THEN 'group_b' ELSE 'other' END AS status_group
FROM sequenced
ORDER BY holder_id DESC
