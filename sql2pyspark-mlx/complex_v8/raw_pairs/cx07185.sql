WITH recent_claims AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        claimed_amount,
        approved_amount,
        ROUND(approved_amount * 1.1, 2) AS approved_amount_adj,
        COALESCE(claimed_amount, 0.0) AS claimed_amount_filled
    FROM claims
    WHERE claimed_amount < 39666.5
),
with_activity AS (
    -- Compare against the overall avg
    SELECT *
    FROM recent_claims
    WHERE claimed_amount > (SELECT AVG(claimed_amount) FROM recent_claims)
),
with_counts AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT MAX(c.reported_at) FROM claims c WHERE c.adjuster_id = s.adjuster_id) AS last_claim_reported_at,
        (SELECT MAX(c.reported_at) FROM claims c WHERE c.adjuster_id = s.adjuster_id) AS last_claim_reported_at_2
    FROM with_activity s
),
enriched AS (
    -- Add calculated columns
    SELECT
        *,
        QUARTER(last_claim_reported_at_2) AS last_claim_reported_at_2_quarter,
        TO_DATE(last_claim_reported_at) AS last_claim_reported_at_date,
        COALESCE(claim_type, 'UNKNOWN') AS claim_type_or_unknown
    FROM with_counts
)
SELECT id, measure, amount
FROM (
    SELECT id, claimed_amount, approved_amount_adj, claimed_amount_filled, approved_amount
    FROM enriched
) src
UNPIVOT (
    amount FOR measure IN (claimed_amount, approved_amount_adj, claimed_amount_filled, approved_amount)
) unp
