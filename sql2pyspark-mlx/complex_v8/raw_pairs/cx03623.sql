WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        approved_amount,
        reported_at
    FROM claims
),
policy_id_stats AS (
    -- Median approved_amount per policy_id
    SELECT policy_id, PERCENTILE_APPROX(approved_amount, 0.5) AS median_approved_amount
    FROM scoped
    GROUP BY policy_id
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_approved_amount,
        ROUND(s.approved_amount - g.median_approved_amount, 2) AS approved_amount_minus_median
    FROM scoped s
    JOIN policy_id_stats g ON g.policy_id = s.policy_id
    WHERE s.approved_amount >= g.median_approved_amount
),
with_children AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claimed_amount,
        s.approved_amount,
        s.reported_at,
        s.median_approved_amount,
        c.id AS claim_id,
        c.reported_at AS claim_reported_at,
        c.approved_amount AS claim_approved_amount,
        c.claimed_amount AS claim_claimed_amount,
        c.status AS claim_status
    FROM vs_group s
    LEFT JOIN claims c ON c.adjuster_id = s.adjuster_id
),
qualified AS (
    -- Compare against the overall avg
    SELECT *
    FROM with_children
    WHERE claimed_amount > (SELECT AVG(claimed_amount) FROM with_children)
)
SELECT
    id,
    policy_id,
    adjuster_id,
    claimed_amount,
    median_approved_amount,
    claim_id,
    claim_reported_at,
    claim_status,
    LOWER(REGEXP_REPLACE(TRIM(claim_status), ' ', '_')) AS claim_status_slug,
    SUBSTRING(TRIM(claim_status), 1, 4) AS claim_status_prefix
FROM qualified
ORDER BY median_approved_amount DESC
