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
        reported_at,
        FLOOR(claimed_amount / 10000) * 10000 AS claimed_amount_bucket,
        CASE WHEN approved_amount >= 21750.2 THEN 'large' WHEN approved_amount >= 799.5 THEN 'medium' ELSE 'small' END AS approved_amount_tier,
        ROUND(approved_amount * 1.1, 2) AS approved_amount_adj
    FROM claims
),
claim_stats AS (
    -- Per-policy claims metrics
    SELECT
        policy_id,
        SUM(claimed_amount) AS total_claim_claimed_amount,
        MIN(claim_date) AS first_claim_date
    FROM claims
    GROUP BY policy_id
),
summarized_rows AS (
    -- Attach claims metrics
    SELECT
        s.*,
        c.total_claim_claimed_amount,
        c.first_claim_date
    FROM cleaned s
    LEFT JOIN claim_stats c ON c.policy_id = s.policy_id
),
claim_stats_2 AS (
    -- Per-policy claims metrics
    SELECT
        policy_id,
        COUNT(*) AS claim_count,
        MAX(approved_amount) AS max_claim_approved_amount
    FROM claims
    GROUP BY policy_id
),
with_stats AS (
    -- Attach claims metrics
    SELECT
        s.*,
        COALESCE(c.claim_count, 0) AS claim_count,
        c.max_claim_approved_amount
    FROM summarized_rows s
    LEFT JOIN claim_stats_2 c ON c.policy_id = s.policy_id
),
combined_details AS (
    -- Attach adjuster attributes
    SELECT s.*, a.team AS adjuster_team
    FROM with_stats s
    LEFT JOIN adjusters a ON a.id = s.adjuster_id
)
-- Final projection
SELECT
    id,
    policy_id,
    claim_date,
    claim_type,
    claimed_amount,
    status,
    reported_at,
    approved_amount_adj,
    total_claim_claimed_amount,
    first_claim_date,
    claim_count,
    max_claim_approved_amount,
    adjuster_team,
    DATE_TRUNC('YEAR', first_claim_date) AS first_claim_date_year,
    DAYOFWEEK(first_claim_date) AS first_claim_date_dayofweek
FROM combined_details
ORDER BY claim_date DESC, policy_id, id
LIMIT 5
