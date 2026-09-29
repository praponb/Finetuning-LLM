WITH valid AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        approved_amount,
        status,
        reported_at,
        ADD_MONTHS(claim_date, 12) AS claim_date_plus_12m,
        SUBSTRING(TRIM(claim_type), 1, 3) AS claim_type_prefix
    FROM claims
    WHERE status <> 'OPEN'
),
with_details AS (
    -- Join policies
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.approved_amount,
        s.reported_at,
        s.claim_type_prefix,
        p.status AS policy_status
    FROM valid s
    LEFT JOIN policies p ON p.id = s.policy_id AND p.premium >= 1051.9
),
derived AS (
    -- Derive helper columns
    SELECT
        id,
        policy_id,
        reported_at,
        policy_status,
        DATE_TRUNC('QUARTER', reported_at) AS reported_at_quarter,
        COALESCE(policy_status, 'UNKNOWN') AS policy_status_or_unknown,
        FLOOR(approved_amount / 10000) * 10000 AS approved_amount_bucket
    FROM with_details
),
combined_details AS (
    -- Join policies
    SELECT s.*, p.holder_id AS policy_holder_id
    FROM derived s
    JOIN policies p ON p.id = s.policy_id
    WHERE p.holder_id IS NOT NULL
)
-- Final projection
SELECT
    id,
    policy_id,
    reported_at,
    policy_status,
    reported_at_quarter,
    policy_status_or_unknown,
    approved_amount_bucket,
    policy_holder_id,
    CONCAT_WS(' / ', policy_status, policy_status_or_unknown) AS policy_status_policy_status_or_unknown_label
FROM combined_details
WHERE policy_holder_id IS NOT NULL
ORDER BY policy_holder_id
