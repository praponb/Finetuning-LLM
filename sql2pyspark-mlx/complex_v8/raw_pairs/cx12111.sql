WITH valid_policys AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        CONCAT(product, '#', CAST(id AS STRING)) AS product_ref
    FROM policies
    WHERE start_date >= DATE '2024-10-22'
      AND holder_id IS NOT NULL
),
detail_rows_claims AS (
    -- One row per claim
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.premium,
        s.start_date,
        s.end_date,
        s.product_ref,
        c.id AS claim_id,
        c.reported_at AS claim_reported_at,
        c.claim_date,
        c.status AS claim_status
    FROM valid_policys s
    JOIN claims c ON c.policy_id = s.id
),
exploded_detail_claims AS (
    -- One row per claim
    SELECT
        s.id,
        s.holder_id,
        s.premium,
        s.start_date,
        s.end_date,
        s.product_ref,
        s.claim_id,
        s.claim_reported_at,
        s.claim_status,
        c.id AS claim_id_2,
        c.adjuster_id AS claim_adjuster_id,
        c.claimed_amount AS claim_claimed_amount,
        c.reported_at AS claim_reported_at_2,
        c.claim_type
    FROM detail_rows_claims s
    JOIN claims c ON c.policy_id = s.id AND c.claimed_amount >= 13635.1
)
-- Subtotals with GROUPING SETS
SELECT
    claim_id,
    COALESCE(claim_type, 'ALL') AS claim_type,
    GROUPING_ID() AS grouping_level,
    PERCENTILE_APPROX(claim_claimed_amount, 0.5) AS median_claim_claimed_amount,
    COUNT(DISTINCT claim_id) AS distinct_claim_ids
FROM exploded_detail_claims
GROUP BY GROUPING SETS ((claim_id, claim_type), (claim_id), ())
ORDER BY claim_id NULLS LAST, claim_type NULLS LAST
