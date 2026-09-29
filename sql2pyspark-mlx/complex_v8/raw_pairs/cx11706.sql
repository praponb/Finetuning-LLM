WITH scoped_claims AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        reported_at,
        ROUND(claimed_amount / NULLIF(approved_amount, 0), 3) AS claimed_amount_per_approved_amount
    FROM claims
    WHERE reported_at >= TIMESTAMP '2025-01-31 13:00:00'
      AND adjuster_id >= 5
),
with_activity AS (
    -- Rows with no matching claims
    SELECT s.*
    FROM scoped_claims s
    WHERE NOT EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.adjuster_id = s.adjuster_id
              AND c.claim_type = 'theft'
              AND c.approved_amount >= 0.0
        )
      AND s.adjuster_id <= 2
),
filtered AS (
    -- Rows with no matching claims
    SELECT s.*
    FROM with_activity s
    WHERE NOT EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.adjuster_id = s.adjuster_id
              AND c.claimed_amount <= 43724.9
        )
      AND s.claimed_amount_per_approved_amount < 7.5
),
with_details_adjuster AS (
    -- Join adjusters
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        a.level AS adjuster_level,
        a.adjuster_name,
        a.team AS adjuster_team,
        p.premium AS policy_premium,
        p.status AS policy_status
    FROM filtered s
    JOIN adjusters a ON a.id = s.adjuster_id
    JOIN policies p ON p.id = s.policy_id
),
screened AS (
    -- Rows with no matching claims
    SELECT s.*
    FROM with_details_adjuster s
    WHERE NOT EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.policy_id
              AND c.approved_amount > 2349.3
              AND c.claim_type <> 'theft'
        )
),
claim_stats AS (
    -- Per-adjuster claims metrics
    SELECT adjuster_id, MIN(approved_amount) AS min_claim_approved_amount
    FROM claims
    GROUP BY adjuster_id
),
summarized_rows AS (
    -- Attach claims metrics
    SELECT
        s.*,
        c.min_claim_approved_amount
    FROM screened s
    LEFT JOIN claim_stats c ON c.adjuster_id = s.adjuster_id
),
combined_details AS (
    -- Attach policy attributes
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.adjuster_level,
        s.adjuster_name,
        s.policy_premium,
        s.policy_status,
        s.min_claim_approved_amount,
        p.start_date AS policy_start_date,
        po.risk_score AS holder_risk_score,
        po.region AS holder_region
    FROM summarized_rows s
    JOIN policies p ON p.id = s.policy_id AND p.start_date < DATE '2025-02-14'
    JOIN policyholders po ON po.id = p.holder_id
    WHERE p.start_date BETWEEN DATE '2024-11-20' AND DATE '2025-03-24'
)
-- Final projection
SELECT
    id,
    policy_id,
    adjuster_id,
    adjuster_level,
    adjuster_name,
    policy_premium,
    policy_start_date,
    holder_risk_score,
    holder_region,
    TRUNC(policy_start_date, 'MM') AS policy_start_date_month_start
FROM combined_details
ORDER BY adjuster_name DESC, holder_risk_score DESC, id
LIMIT 5
