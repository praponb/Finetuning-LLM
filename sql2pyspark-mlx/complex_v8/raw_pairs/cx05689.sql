WITH scoped AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        end_date,
        status
    FROM policies
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        holder_id,
        product,
        premium,
        end_date,
        NTILE(3) OVER (PARTITION BY holder_id, product ORDER BY premium DESC, id) AS premium_tercile,
        ROUND(CUME_DIST() OVER w_holder_id_product_by_premium_2, 3) AS premium_cume_dist,
        ROUND(premium - AVG(premium) OVER (PARTITION BY status), 2) AS premium_vs_status_avg
    FROM scoped
    WINDOW w_holder_id_product_by_premium_2 AS (PARTITION BY holder_id, product ORDER BY premium DESC)
),
unique_rows AS (
    -- Keep the latest row per product
    SELECT
        id,
        holder_id,
        product,
        premium,
        end_date,
        premium_tercile,
        premium_cume_dist,
        premium_vs_status_avg
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY product ORDER BY end_date DESC, id DESC) AS recency_rank
        FROM with_trends
    ) ranked
    WHERE recency_rank = 1
),
labeled AS (
    -- Label and score rows
    SELECT
        *,
        LOWER(REGEXP_REPLACE(TRIM(product), ' ', '_')) AS product_slug,
        ABS(premium_cume_dist - 0.3) AS premium_cume_dist_distance,
        ROUND(premium * 1.07, 2) AS premium_adj
    FROM unique_rows
),
line_level AS (
    -- One row per claim
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.premium,
        s.premium_tercile,
        s.premium_vs_status_avg,
        s.product_slug,
        s.premium_cume_dist_distance,
        s.premium_adj,
        c.id AS claim_id,
        c.status AS claim_status,
        c.reported_at AS claim_reported_at,
        c.approved_amount AS claim_approved_amount
    FROM labeled s
    JOIN claims c ON c.policy_id = s.id AND c.status = 'PAID'
),
qualified AS (
    -- Rows with no matching claims
    SELECT s.*
    FROM line_level s
    WHERE NOT EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.id
              AND c.claim_date < DATE '2025-04-14'
              AND c.status <> 'IN_REVIEW'
        )
      AND s.holder_id >= 9
),
enriched_claim AS (
    -- Join claims
    SELECT
        s.*,
        c.claimed_amount AS claim_claimed_amount,
        c.claim_type,
        c.reported_at AS claim_reported_at_2
    FROM qualified s
    JOIN claims c ON c.id = s.claim_id
)
-- Final projection
SELECT
    id,
    product,
    premium,
    premium_tercile,
    premium_cume_dist_distance,
    premium_adj,
    claim_status,
    claim_reported_at,
    claim_approved_amount,
    claim_claimed_amount,
    claim_type,
    claim_reported_at_2,
    ABS(claim_approved_amount - 14217.6) AS claim_approved_amount_distance
FROM enriched_claim
ORDER BY premium_adj DESC, claim_claimed_amount DESC
