WITH scoped AS (
    -- Filter claims
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
        LEAST(claimed_amount, approved_amount) AS least_claimed_amount_approved_amount,
        DATE_TRUNC('MONTH', claim_date) AS claim_date_month
    FROM claims
    WHERE approved_amount > 2272.7
),
combined_details AS (
    -- Join policies
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.status,
        s.least_claimed_amount_approved_amount,
        s.claim_date_month,
        p.premium AS policy_premium,
        a.level AS adjuster_level,
        a.adjuster_name
    FROM scoped s
    JOIN policies p ON p.id = s.policy_id AND YEAR(p.start_date) = 2025
    LEFT JOIN adjusters a ON a.id = s.adjuster_id
),
with_parent_policy AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        p.end_date AS policy_end_date,
        p.product AS policy_product,
        p.premium AS policy_premium_2
    FROM combined_details s
    JOIN policies p ON p.id = s.policy_id
),
summary AS (
    -- Summarise per group
    SELECT
        adjuster_id,
        DATE_TRUNC('MONTH', claim_date_month) AS claim_date_month_month,
        ROUND(SUM(policy_premium_2) / NULLIF(SUM(policy_premium), 0), 3) AS policy_premium_2_to_policy_premium_ratio,
        COUNT(*) AS n_rows,
        COUNT(DISTINCT id) AS distinct_ids
    FROM with_parent_policy
    GROUP BY 1, 2
    HAVING MAX(least_claimed_amount_approved_amount) > 12500.0
),
combined_details_adjuster AS (
    -- Enrich with dimension data
    SELECT
        s.adjuster_id,
        s.claim_date_month_month,
        s.policy_premium_2_to_policy_premium_ratio,
        s.n_rows,
        a.team AS adjuster_team,
        a.level AS adjuster_level
    FROM summary s
    LEFT JOIN adjusters a ON a.id = s.adjuster_id
)
-- Final projection
SELECT
    adjuster_id,
    claim_date_month_month,
    policy_premium_2_to_policy_premium_ratio,
    n_rows,
    adjuster_team,
    adjuster_level
FROM combined_details_adjuster
ORDER BY adjuster_team NULLS LAST, adjuster_id, claim_date_month_month
LIMIT 10
