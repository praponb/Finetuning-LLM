WITH filtered_policys AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        end_date,
        status
    FROM policies
    WHERE start_date < DATE '2025-02-13'
      AND status IN ('CANCELLED', 'ACTIVE', 'EXPIRED')
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        MAX(premium) OVER (ORDER BY premium, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS premium_max_2,
        NTILE(10) OVER (ORDER BY premium, id) AS premium_decile,
        MIN(premium) OVER (PARTITION BY holder_id, status) AS holder_id_status_min_premium
    FROM filtered_policys
),
deduped AS (
    -- Keep the latest row per holder_id, product
    SELECT
        id,
        holder_id,
        product,
        premium,
        end_date,
        status,
        premium_max_2,
        premium_decile,
        holder_id_status_min_premium
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY holder_id, product ORDER BY premium_max_2 DESC, id DESC) AS rn
        FROM metrics
    ) ranked
    WHERE rn = 1
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        id,
        holder_id,
        end_date,
        status,
        holder_id_status_min_premium,
        NTILE(5) OVER (PARTITION BY premium_decile ORDER BY end_date ASC, id) AS end_date_quintile,
        LAST_VALUE(end_date) OVER (PARTITION BY premium_decile ORDER BY end_date ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_end_date,
        MAX(premium) OVER (PARTITION BY holder_id) AS holder_id_max_premium
    FROM deduped
)
-- Final projection
SELECT DISTINCT holder_id_max_premium, id
FROM with_trends
ORDER BY id
