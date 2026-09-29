WITH filtered_policys AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        end_date
    FROM policies
    WHERE holder_id <= 4
      AND status = 'ACTIVE'
),
metrics AS (
    -- Previous / next values per key
    SELECT
        id,
        holder_id,
        product,
        premium,
        ROUND(AVG(premium) OVER (PARTITION BY DATE_TRUNC('MONTH', end_date), product ORDER BY end_date DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS premium_avg_2,
        SUM(premium) OVER (PARTITION BY DATE_TRUNC('MONTH', end_date), product ORDER BY end_date DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS premium_sum_3,
        MAX(premium) OVER (PARTITION BY product, holder_id ORDER BY premium DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_premium_so_far,
        DATEDIFF(end_date, LAG(end_date) OVER (PARTITION BY DATE_TRUNC('MONTH', end_date), product ORDER BY end_date DESC, id)) AS days_since_prev_end_date,
        ROUND(premium - AVG(premium) OVER (PARTITION BY product, holder_id), 2) AS premium_vs_product_holder_id_avg
    FROM filtered_policys
),
latest_per_key AS (
    -- Keep the latest row per product
    SELECT
        id,
        holder_id,
        product,
        premium,
        premium_avg_2,
        premium_sum_3,
        max_premium_so_far,
        days_since_prev_end_date,
        premium_vs_product_holder_id_avg
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY product ORDER BY premium DESC, id DESC) AS row_num
        FROM metrics
    ) ranked
    WHERE row_num = 1
),
qualified AS (
    -- Rows with no matching claims
    SELECT s.*
    FROM latest_per_key s
    WHERE NOT EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.id
              AND c.adjuster_id IS NOT NULL
              AND c.claim_type IS NOT NULL
        )
),
filtered AS (
    -- Rows with no matching policies
    SELECT s.*
    FROM qualified s
    WHERE NOT EXISTS (
            SELECT 1
            FROM policies p
            WHERE p.holder_id = s.holder_id
              AND p.end_date BETWEEN DATE '2025-10-12' AND DATE '2026-12-05'
        )
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        holder_id,
        premium,
        premium_avg_2,
        max_premium_so_far,
        days_since_prev_end_date,
        RANK() OVER (PARTITION BY product ORDER BY premium_sum_3) AS premium_sum_3_rank,
        max_premium_so_far - LAG(max_premium_so_far) OVER w_product_by_premium_sum_3 AS max_premium_so_far_change,
        SUM(premium_sum_3) OVER (PARTITION BY holder_id, product) AS holder_id_product_sum_premium_sum_3,
        MIN(max_premium_so_far) OVER (PARTITION BY holder_id, product) AS holder_id_product_min_max_premium_so_far,
        SUM(max_premium_so_far) OVER (PARTITION BY product ORDER BY premium_sum_3, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS max_premium_so_far_sum_6_prior,
        SUM(days_since_prev_end_date) OVER (PARTITION BY holder_id, product) AS holder_id_product_sum_days_since_prev_end_date
    FROM filtered
    WINDOW w_product_by_premium_sum_3 AS (PARTITION BY product ORDER BY premium_sum_3, id)
),
enriched AS (
    -- Attach holder attributes
    SELECT
        s.id,
        s.holder_id,
        s.premium_avg_2,
        s.max_premium_so_far,
        s.premium_sum_3_rank,
        s.max_premium_so_far_sum_6_prior,
        s.holder_id_product_sum_days_since_prev_end_date,
        p.risk_score AS holder_risk_score,
        p.region AS holder_region
    FROM with_windows s
    JOIN policyholders p ON p.id = s.holder_id AND p.segment IS NOT NULL
)
SELECT
    id,
    holder_id,
    premium_avg_2,
    max_premium_so_far,
    premium_sum_3_rank,
    max_premium_so_far_sum_6_prior,
    holder_id_product_sum_days_since_prev_end_date,
    holder_risk_score,
    holder_region
FROM enriched
ORDER BY holder_id DESC, holder_risk_score DESC NULLS FIRST
