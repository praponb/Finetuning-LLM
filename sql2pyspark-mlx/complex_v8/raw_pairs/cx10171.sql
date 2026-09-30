WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        claimed_amount,
        status,
        reported_at,
        GREATEST(approved_amount, claimed_amount) AS greatest_approved_amount_claimed_amount,
        ABS(claimed_amount - 29336.0) AS claimed_amount_distance
    FROM claims
    WHERE MONTH(reported_at) IN (1, 3)
      AND claim_type NOT IN ('delay', 'theft')
),
profiled AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT MIN(c.claim_date) FROM claims c WHERE c.adjuster_id = s.adjuster_id) AS first_claim_date,
        (SELECT COUNT(CASE WHEN c.claim_type <> 'medical' THEN 1 END) FROM claims c WHERE c.adjuster_id = s.adjuster_id) AS claim_claim_type_count,
        (SELECT SUM(c.claimed_amount) FROM claims c WHERE c.adjuster_id = s.adjuster_id) AS total_claim_claimed_amount
    FROM base s
),
joined AS (
    -- Attach policy attributes
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_type,
        s.claimed_amount,
        s.status,
        s.reported_at,
        s.greatest_approved_amount_claimed_amount,
        s.claimed_amount_distance,
        s.first_claim_date,
        s.claim_claim_type_count,
        s.total_claim_claimed_amount,
        p.end_date AS policy_end_date,
        p.holder_id AS policy_holder_id
    FROM profiled s
    JOIN policies p ON p.id = s.policy_id AND p.end_date IS NOT NULL
),
line_level AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claimed_amount,
        s.status,
        s.reported_at,
        s.greatest_approved_amount_claimed_amount,
        s.first_claim_date,
        s.total_claim_claimed_amount,
        s.policy_holder_id,
        c.id AS claim_id,
        c.claim_type AS claim_claim_type,
        c.claim_date,
        c.status AS claim_status,
        c.adjuster_id AS claim_adjuster_id
    FROM joined s
    JOIN claims c ON c.policy_id = s.policy_id
),
latest_per_key AS (
    -- Keep the latest row per adjuster_id, policy_holder_id
    SELECT
        id,
        policy_id,
        adjuster_id,
        claimed_amount,
        status,
        reported_at,
        greatest_approved_amount_claimed_amount,
        first_claim_date,
        total_claim_claimed_amount,
        policy_holder_id,
        claim_id,
        claim_claim_type,
        claim_date,
        claim_status,
        claim_adjuster_id
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY adjuster_id, policy_holder_id ORDER BY claim_date DESC, id DESC, claim_id DESC) AS recency_rank
        FROM line_level
    ) ranked
    WHERE recency_rank = 1
),
with_details AS (
    -- Enrich with dimension data
    SELECT s.*, p.region AS holder_region, p.joined_on AS holder_joined_on
    FROM latest_per_key s
    LEFT JOIN policyholders p ON p.id = s.policy_holder_id
)
SELECT
    id,
    policy_id,
    adjuster_id,
    claimed_amount,
    reported_at,
    greatest_approved_amount_claimed_amount,
    total_claim_claimed_amount,
    policy_holder_id,
    claim_id,
    claim_claim_type,
    claim_date,
    claim_status,
    holder_region,
    holder_joined_on,
    reported_at + INTERVAL 7 DAYS AS reported_at_plus_7d
FROM with_details
WHERE total_claim_claimed_amount BETWEEN 45085.0 AND 240020.0
ORDER BY greatest_approved_amount_claimed_amount, holder_joined_on
