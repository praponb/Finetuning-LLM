WITH base_claims AS (
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
        CONCAT(status, '#', CAST(id AS STRING)) AS status_ref,
        ABS(claimed_amount - 19337.6) AS claimed_amount_distance,
        LEAST(claimed_amount, approved_amount) AS least_claimed_amount_approved_amount
    FROM claims
),
annotated AS (
    -- Derive helper columns
    SELECT *, UPPER(TRIM(status_ref)) AS status_ref_clean
    FROM base_claims
),
claim_stats AS (
    -- Per-policy claims metrics
    SELECT
        policy_id,
        MIN(reported_at) AS first_claim_reported_at,
        COUNT(*) AS claim_count
    FROM claims
    GROUP BY policy_id
),
profiled AS (
    -- Attach claims metrics
    SELECT
        s.*,
        c.first_claim_reported_at,
        COALESCE(c.claim_count, 0) AS claim_count
    FROM annotated s
    LEFT JOIN claim_stats c ON c.policy_id = s.policy_id
),
joined_policy AS (
    -- Join policies
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.approved_amount,
        s.status,
        s.reported_at,
        s.status_ref,
        s.claimed_amount_distance,
        s.least_claimed_amount_approved_amount,
        s.status_ref_clean,
        s.first_claim_reported_at,
        s.claim_count,
        p.holder_id AS policy_holder_id,
        p.premium AS policy_premium,
        a.adjuster_name
    FROM profiled s
    JOIN policies p ON p.id = s.policy_id
    LEFT JOIN adjusters a ON a.id = s.adjuster_id
)
SELECT
    id,
    policy_id,
    adjuster_id,
    status,
    reported_at,
    status_ref,
    claimed_amount_distance,
    least_claimed_amount_approved_amount,
    first_claim_reported_at,
    claim_count,
    ROUND((UNIX_TIMESTAMP(reported_at) - UNIX_TIMESTAMP(first_claim_reported_at)) / 3600, 1) AS hours_first_claim_reported_at_to_reported_at,
    ROUND(claim_count / NULLIF(policy_premium, 0), 3) AS claim_count_per_policy_premium
FROM joined_policy
