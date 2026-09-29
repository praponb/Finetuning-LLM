WITH recent AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        end_date,
        status,
        ABS(premium - 2111.3) AS premium_distance
    FROM policies
    WHERE holder_id IS NOT NULL
      AND premium > 2111.3
),
enriched_holder AS (
    -- Join policyholders
    SELECT s.*, p.region AS holder_region
    FROM recent s
    LEFT JOIN policyholders p ON p.id = s.holder_id
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        product,
        premium,
        end_date,
        premium_distance,
        holder_region,
        ROUND(premium - AVG(premium) OVER w_product_status, 2) AS premium_vs_product_status_avg,
        LEAD(premium_distance, 2) OVER (PARTITION BY status, holder_region ORDER BY end_date ASC, id) AS next2_premium_distance,
        COUNT(*) OVER (PARTITION BY product, status) AS rows_in_product_status,
        MAX(premium_distance) OVER (PARTITION BY status, holder_region ORDER BY end_date ASC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS premium_distance_max_4,
        RANK() OVER (PARTITION BY status, holder_region ORDER BY end_date ASC, id) AS end_date_rank,
        SIZE(COLLECT_SET(holder_id) OVER (PARTITION BY product, status)) AS distinct_holder_id_in_product_status,
        SUM(premium) OVER (PARTITION BY status, holder_region ORDER BY end_date ASC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS premium_sum_1_prior
    FROM enriched_holder
    WINDOW w_product_status AS (PARTITION BY product, status)
),
top_rows AS (
    -- Top 2 per group
    SELECT *
    FROM with_windows
    WHERE end_date_rank <= 2
      AND YEAR(end_date) = 2026
)
SELECT
    id,
    premium,
    end_date,
    premium_distance,
    holder_region,
    next2_premium_distance,
    rows_in_product_status,
    premium_distance_max_4,
    distinct_holder_id_in_product_status,
    premium_sum_1_prior,
    CASE WHEN premium >= 2945.9 THEN 'large' WHEN premium >= 2033.8 THEN 'medium' ELSE 'small' END AS premium_tier,
    GREATEST(premium_sum_1_prior, next2_premium_distance) AS greatest_premium_sum_1_prior_next2_premium_dista
FROM top_rows
ORDER BY premium DESC
