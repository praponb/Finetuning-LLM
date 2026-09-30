WITH cleaned_claims AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        status,
        reported_at,
        DATEDIFF(DATE '2025-06-30', claim_date) AS days_since_claim_date,
        DATEDIFF(DATE '2025-06-30', claim_date) AS days_since_claim_date_2,
        ABS(claimed_amount - 32441.2) AS claimed_amount_distance
    FROM claims
    WHERE claimed_amount >= 18778.6
),
summarized_rows AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FILTER (WHERE c.claim_type NOT IN ('fire', 'medical')) FROM claims c WHERE c.policy_id = s.policy_id) AS claim_claim_type_count,
        (SELECT MAX(c.reported_at) FROM claims c WHERE c.policy_id = s.policy_id) AS last_claim_reported_at,
        (SELECT COUNT(*) FROM claims c WHERE c.policy_id = s.policy_id) AS claim_count
    FROM cleaned_claims s
),
claim_stats_2 AS (
    -- Per-policy claims metrics
    SELECT
        policy_id,
        COUNT(*) AS claim_count_2,
        MIN(reported_at) AS first_claim_reported_at,
        MIN(approved_amount) AS min_claim_approved_amount
    FROM claims
    GROUP BY policy_id
),
with_counts AS (
    -- Attach claims metrics
    SELECT
        s.*,
        COALESCE(c.claim_count_2, 0) AS claim_count_2,
        c.first_claim_reported_at,
        c.min_claim_approved_amount
    FROM summarized_rows s
    LEFT JOIN claim_stats_2 c ON c.policy_id = s.policy_id
),
combined_details AS (
    -- Join policies
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claim_type,
        s.status,
        s.reported_at,
        s.days_since_claim_date,
        s.days_since_claim_date_2,
        s.claimed_amount_distance,
        s.claim_claim_type_count,
        s.claim_count,
        s.claim_count_2,
        s.first_claim_reported_at,
        s.min_claim_approved_amount,
        p.product AS policy_product
    FROM with_counts s
    JOIN policies p ON p.id = s.policy_id
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, metric, value
FROM (
    SELECT id, claimed_amount_distance, min_claim_approved_amount
    FROM combined_details
) src
UNPIVOT (
    value FOR metric IN (claimed_amount_distance, min_claim_approved_amount)
) unp
ORDER BY id, metric
