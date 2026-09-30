WITH scoped AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        status,
        reported_at,
        LOWER(REGEXP_REPLACE(TRIM(claim_type), ' ', '_')) AS claim_type_slug,
        YEAR(claim_date) AS claim_date_year,
        ROUND(claimed_amount * 0.25, 2) AS claimed_amount_adj
    FROM claims
    WHERE adjuster_id IS NOT NULL
      AND policy_id IS NOT NULL
),
derived AS (
    -- Derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        reported_at,
        claim_type_slug,
        claimed_amount_adj,
        CASE WHEN claimed_amount >= 31715.7 THEN 'large' WHEN claimed_amount >= 17759.3 THEN 'medium' ELSE 'small' END AS claimed_amount_tier,
        COALESCE(claimed_amount, 0.0) AS claimed_amount_filled,
        ROUND(claimed_amount_adj * 1.07, 2) AS claimed_amount_adj_adj
    FROM scoped
)
SELECT id, measure, value
FROM (
    SELECT id, claimed_amount, claimed_amount_filled, claimed_amount_adj_adj
    FROM derived
) src
UNPIVOT (
    value FOR measure IN (claimed_amount AS claimed_amount_v, claimed_amount_filled AS m_claimed_amount_filled, claimed_amount_adj_adj AS m_claimed_amount_adj_adj)
) unp
