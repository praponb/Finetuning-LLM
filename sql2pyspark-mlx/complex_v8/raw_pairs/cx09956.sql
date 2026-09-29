WITH base AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        reported_at
    FROM claims
    WHERE claim_type = 'medical'
      AND claimed_amount > 18924.4
),
combined_details AS (
    -- Join policies
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claim_type,
        s.claimed_amount,
        s.reported_at,
        p.start_date AS policy_start_date,
        p.status AS policy_status,
        p.product AS policy_product
    FROM base s
    LEFT JOIN policies p ON p.id = s.policy_id
),
derived AS (
    -- Add calculated columns
    SELECT
        *,
        DATE_FORMAT(reported_at, 'MMM yyyy') AS reported_at_label,
        COALESCE(claim_type, 'UNKNOWN') AS claim_type_or_unknown
    FROM combined_details
    WHERE adjuster_id <= 3
),
labeled AS (
    -- Label and score rows
    SELECT
        *,
        CONCAT(claim_type_or_unknown, '#', CAST(adjuster_id AS STRING)) AS claim_type_or_unknown_ref,
        CONCAT_WS(' / ', policy_status, claim_type) AS policy_status_claim_type_label
    FROM derived
)
-- Subtotals with ROLLUP
SELECT
    policy_id,
    claim_type_or_unknown,
    policy_product,
    MIN(claimed_amount) AS min_claimed_amount,
    SUM(CASE WHEN claim_type <> 'fire' THEN claimed_amount ELSE 0 END) AS claimed_amount_claim_type_fire,
    MAX(claim_date) AS last_claim_date
FROM labeled
GROUP BY ROLLUP(policy_id, claim_type_or_unknown, policy_product)
ORDER BY policy_id NULLS LAST, claim_type_or_unknown NULLS LAST, policy_product NULLS LAST
