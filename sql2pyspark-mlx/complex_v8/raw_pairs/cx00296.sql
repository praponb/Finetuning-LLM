WITH cleaned AS (
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
        reported_at
    FROM claims
    WHERE claim_type IS NOT NULL
),
enriched_policy AS (
    -- Attach policy attributes
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claim_type,
        s.approved_amount,
        s.status,
        p.start_date AS policy_start_date
    FROM cleaned s
    LEFT JOIN policies p ON p.id = s.policy_id
),
claim_type_stats AS (
    -- Median approved_amount per claim_type
    SELECT claim_type, PERCENTILE_APPROX(approved_amount, 0.5) AS median_approved_amount
    FROM enriched_policy
    GROUP BY claim_type
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_approved_amount,
        ROUND(s.approved_amount - g.median_approved_amount, 2) AS approved_amount_minus_median
    FROM enriched_policy s
    JOIN claim_type_stats g ON g.claim_type = s.claim_type
    WHERE s.approved_amount >= g.median_approved_amount
)
SELECT
    id,
    policy_id,
    adjuster_id,
    claim_type,
    approved_amount,
    policy_start_date,
    median_approved_amount
FROM outliers
