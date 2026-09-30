WITH scoped AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status
    FROM policies
    WHERE status = 'LAPSED'
),
claim_stats AS (
    -- Per-policy claims metrics
    SELECT
        policy_id,
        COUNT(*) FILTER (WHERE claim_type IN ('fire', 'collision', 'medical')) AS claim_claim_type_count,
        COUNT(*) AS claim_count,
        SUM(approved_amount) AS total_claim_approved_amount
    FROM claims
    GROUP BY policy_id
),
with_counts AS (
    -- Attach claims metrics
    SELECT
        s.*,
        COALESCE(c.claim_claim_type_count, 0) AS claim_claim_type_count,
        COALESCE(c.claim_count, 0) AS claim_count,
        c.total_claim_approved_amount
    FROM scoped s
    LEFT JOIN claim_stats c ON c.policy_id = s.id
),
tagged AS (
    -- Add calculated columns
    SELECT
        id,
        product,
        premium,
        start_date,
        claim_claim_type_count,
        claim_count,
        total_claim_approved_amount,
        claim_count + premium AS claim_count_plus_premium,
        ROUND(premium * 1.07, 2) AS premium_adj
    FROM with_counts
),
derived AS (
    -- Label and score rows
    SELECT
        id,
        premium,
        start_date,
        claim_claim_type_count,
        total_claim_approved_amount,
        claim_count_plus_premium,
        premium_adj,
        COALESCE(premium, 0.0) AS premium_filled,
        CASE WHEN claim_count_plus_premium >= 3004.8 THEN 'large' WHEN claim_count_plus_premium >= 1252.0 THEN 'medium' ELSE 'small' END AS claim_count_plus_premium_tier
    FROM tagged
)
SELECT
    id,
    premium,
    start_date,
    claim_claim_type_count,
    claim_count_plus_premium,
    premium_adj,
    premium_filled,
    claim_count_plus_premium_tier,
    LEAST(premium, premium_filled) AS least_premium_premium_filled,
    DAYOFWEEK(start_date) AS start_date_dayofweek
FROM derived
ORDER BY start_date_dayofweek DESC
