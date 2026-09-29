WITH cleaned_claims AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        status,
        reported_at,
        LEAST(claimed_amount, approved_amount) AS least_claimed_amount_approved_amount,
        SUBSTRING(TRIM(claim_type), 1, 4) AS claim_type_prefix
    FROM claims
    WHERE claimed_amount <= 44921.3
),
joined_adjuster AS (
    -- Attach adjuster attributes
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.status,
        s.reported_at,
        s.least_claimed_amount_approved_amount,
        a.level AS adjuster_level,
        a.adjuster_name,
        p.start_date AS policy_start_date,
        p.product AS policy_product
    FROM cleaned_claims s
    LEFT JOIN adjusters a ON a.id = s.adjuster_id
    LEFT JOIN policies p ON p.id = s.policy_id
),
filtered AS (
    -- Rows with no matching claims
    SELECT s.*
    FROM joined_adjuster s
    WHERE NOT EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.policy_id
              AND c.approved_amount <= 23487.5
        )
),
detail_rows AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.reported_at,
        s.least_claimed_amount_approved_amount,
        s.policy_start_date,
        c.id AS claim_id,
        c.policy_id AS claim_policy_id,
        c.reported_at AS claim_reported_at,
        c.status AS claim_status
    FROM filtered s
    LEFT JOIN claims c ON c.adjuster_id = s.adjuster_id AND c.policy_id <= 12
),
claim_stats AS (
    -- Per-policy claims metrics
    SELECT
        policy_id,
        MIN(claim_date) AS first_claim_date,
        MIN(claim_date) AS first_claim_date_2,
        MAX(claim_date) AS last_claim_date
    FROM claims
    GROUP BY policy_id
),
summarized_rows AS (
    -- Attach claims metrics
    SELECT
        s.*,
        c.first_claim_date,
        c.first_claim_date_2,
        c.last_claim_date
    FROM detail_rows s
    LEFT JOIN claim_stats c ON c.policy_id = s.claim_policy_id
)
-- Final projection
SELECT
    id,
    policy_id,
    adjuster_id,
    reported_at,
    least_claimed_amount_approved_amount,
    claim_id,
    claim_policy_id,
    claim_reported_at,
    claim_status,
    first_claim_date,
    first_claim_date_2,
    DATE_TRUNC('MONTH', first_claim_date_2) AS first_claim_date_2_month
FROM summarized_rows
ORDER BY reported_at, id, claim_id
LIMIT 5
