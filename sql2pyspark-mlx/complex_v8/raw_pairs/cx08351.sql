WITH filtered AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        start_date,
        end_date,
        status,
        SUBSTRING(TRIM(product), 1, 3) AS product_prefix,
        ABS(premium - 3521.8) AS premium_distance,
        ROUND(premium * 1.1, 2) AS premium_adj
    FROM policies
    WHERE holder_id <= 16
),
filtered_2 AS (
    -- Rows with no matching claims
    SELECT s.*
    FROM filtered s
    WHERE NOT EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.id
              AND c.reported_at < TIMESTAMP '2025-02-24 07:00:00'
        )
      AND s.product = 'AUTO'
),
screened AS (
    -- Rows that have matching policies
    SELECT s.*
    FROM filtered_2 s
    WHERE EXISTS (
            SELECT 1
            FROM policies p
            WHERE p.holder_id = s.holder_id
              AND p.premium > 2033.8
        )
      AND s.premium_distance BETWEEN 1250.0 AND 3000.0
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(premium_distance - AVG(premium_distance) OVER (PARTITION BY product), 2) AS premium_distance_vs_product_avg,
        ROUND(AVG(premium_adj) OVER (PARTITION BY product ORDER BY end_date, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS premium_adj_avg_3,
        ROUND(premium_distance / NULLIF(SUM(premium_distance) OVER (PARTITION BY product), 0), 4) AS premium_distance_share_of_product,
        RANK() OVER (PARTITION BY status, DATE_TRUNC('MONTH', end_date) ORDER BY premium_adj DESC, id) AS premium_adj_rank,
        ROUND(premium_adj - AVG(premium_adj) OVER w_product, 2) AS premium_adj_vs_product_avg,
        ROUND(AVG(premium_adj) OVER (PARTITION BY status, DATE_TRUNC('MONTH', end_date) ORDER BY premium_adj DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS premium_adj_avg_3_prior
    FROM screened
    WINDOW w_product AS (PARTITION BY product)
),
claim_stats AS (
    -- Per-policy claims metrics
    SELECT
        policy_id,
        MIN(reported_at) AS first_claim_reported_at,
        MIN(claimed_amount) AS min_claim_claimed_amount
    FROM claims
    GROUP BY policy_id
),
with_stats AS (
    -- Attach claims metrics
    SELECT
        s.*,
        c.first_claim_reported_at,
        c.min_claim_claimed_amount
    FROM ranked s
    LEFT JOIN claim_stats c ON c.policy_id = s.id
),
joined AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.holder_id,
        s.start_date,
        s.end_date,
        s.product_prefix,
        s.premium_distance,
        s.premium_distance_vs_product_avg,
        s.premium_adj_avg_3,
        s.premium_adj_rank,
        s.premium_adj_vs_product_avg,
        s.premium_adj_avg_3_prior,
        s.first_claim_reported_at,
        s.min_claim_claimed_amount,
        p.risk_score AS holder_risk_score
    FROM with_stats s
    JOIN policyholders p ON p.id = s.holder_id
),
with_details AS (
    -- Attach holder attributes
    SELECT
        s.id,
        s.holder_id,
        s.start_date,
        s.end_date,
        s.product_prefix,
        s.premium_adj_avg_3,
        s.premium_adj_rank,
        s.premium_adj_avg_3_prior,
        s.holder_risk_score,
        p.segment AS holder_segment
    FROM joined s
    JOIN policyholders p ON p.id = s.holder_id
)
-- Final projection
SELECT
    id,
    holder_id,
    start_date,
    end_date,
    product_prefix,
    premium_adj_avg_3_prior,
    holder_segment
FROM with_details
WHERE premium_adj_rank > 3
ORDER BY holder_id
