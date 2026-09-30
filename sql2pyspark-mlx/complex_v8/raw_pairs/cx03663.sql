WITH cleaned_policys AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        status,
        COALESCE(premium, 0.0) AS premium_filled,
        CASE WHEN premium >= 3817.5 THEN 'HIGH' WHEN premium >= 2033.8 THEN 'MEDIUM' ELSE 'LOW' END AS premium_tier
    FROM policies
    WHERE premium >= 1541.8
      AND end_date >= DATE '2026-01-24'
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        product,
        premium,
        start_date,
        premium_tier,
        LEAD(start_date) OVER (ORDER BY premium DESC, id) AS next_start_date,
        ROUND(PERCENT_RANK() OVER w_all_by_premium, 3) AS premium_percent_rank,
        DENSE_RANK() OVER w_all_by_premium AS premium_dense_rank,
        LAG(premium_filled, 2, 0) OVER w_product_premium_tier_by_star AS prev2_premium_filled
    FROM cleaned_policys
    WINDOW w_all_by_premium AS (ORDER BY premium DESC, id), w_product_premium_tier_by_star AS (PARTITION BY product, premium_tier ORDER BY start_date DESC, id)
),
filtered AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM metrics s
    WHERE EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.id
        )
),
rollup_base AS (
    -- Aggregate with conditional metrics
    SELECT product, SUM(premium) AS total_premium, COUNT(*) AS cnt
    FROM filtered
    GROUP BY product
    HAVING MAX(premium) > 1051.9
)
-- Final projection
SELECT product, total_premium, cnt
FROM rollup_base
WHERE product IN ('HEALTH', 'TRAVEL')
ORDER BY product NULLS LAST, cnt DESC NULLS FIRST
