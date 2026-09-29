WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        approved_amount,
        reported_at,
        CONCAT(status, '#', CAST(id AS STRING)) AS status_ref,
        LENGTH(TRIM(claim_type)) AS claim_type_len
    FROM claims
    WHERE adjuster_id IS NOT NULL
      AND reported_at >= TIMESTAMP '2025-01-25 14:00:00'
),
latest AS (
    -- Keep the latest row per adjuster_id
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        approved_amount,
        reported_at,
        status_ref,
        claim_type_len
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY adjuster_id ORDER BY claim_type_len DESC, id DESC) AS row_num
        FROM filtered
    ) ranked
    WHERE row_num = 1
),
joined_policy AS (
    -- Join policies
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_type,
        s.approved_amount,
        s.reported_at,
        s.status_ref,
        s.claim_type_len,
        p.premium AS policy_premium,
        p.product AS policy_product
    FROM latest s
    JOIN policies p ON p.id = s.policy_id AND p.product IN ('LIFE', 'HOME', 'TRAVEL')
),
claim_stats AS (
    -- Per-policy claims metrics
    SELECT policy_id, MIN(approved_amount) AS min_claim_approved_amount
    FROM claims
    GROUP BY policy_id
),
summarized_rows AS (
    -- Attach claims metrics
    SELECT
        s.*,
        c.min_claim_approved_amount
    FROM joined_policy s
    LEFT JOIN claim_stats c ON c.policy_id = s.policy_id
),
scored AS (
    -- Label and score rows
    SELECT
        *,
        INITCAP(TRIM(status_ref)) AS status_ref_clean,
        policy_premium + approved_amount AS policy_premium_plus_approved_amount
    FROM summarized_rows
),
line_level AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_type,
        s.approved_amount,
        s.policy_premium,
        s.policy_product,
        s.min_claim_approved_amount,
        c.id AS claim_id,
        c.status AS claim_status,
        c.reported_at AS claim_reported_at,
        c.claim_date,
        c.approved_amount AS claim_approved_amount
    FROM scored s
    LEFT JOIN claims c ON c.policy_id = s.policy_id
)
SELECT
    id,
    policy_id,
    adjuster_id,
    claim_type,
    approved_amount,
    policy_premium,
    policy_product,
    min_claim_approved_amount,
    claim_status,
    claim_reported_at,
    claim_date,
    claim_approved_amount,
    CASE WHEN claim_status IN ('OPEN', 'IN_REVIEW') THEN 'group_a' WHEN claim_status = 'APPROVED' THEN 'group_b' ELSE 'other' END AS claim_status_group,
    CONCAT_WS('|', claim_type, policy_product) AS claim_type_policy_product_label
FROM line_level
ORDER BY min_claim_approved_amount DESC
