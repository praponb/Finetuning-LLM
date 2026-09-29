WITH recent_claims AS (
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
        SUBSTRING(TRIM(status), 1, 3) AS status_prefix,
        CONCAT_WS('-', status, claim_type) AS status_claim_type_label,
        LENGTH(TRIM(status)) AS status_len
    FROM claims
    WHERE adjuster_id IS NOT NULL
      AND claim_type = 'fire'
),
claim_stats AS (
    -- Per-policy claims metrics
    SELECT
        policy_id,
        MAX(claim_date) AS last_claim_date,
        MAX(claim_date) AS last_claim_date_2
    FROM claims
    GROUP BY policy_id
),
with_stats AS (
    -- Attach claims metrics
    SELECT
        s.*,
        c.last_claim_date,
        c.last_claim_date_2
    FROM recent_claims s
    LEFT JOIN claim_stats c ON c.policy_id = s.policy_id
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        SIZE(COLLECT_SET(claim_type) OVER (PARTITION BY policy_id)) AS distinct_claim_type_in_policy_id,
        SIZE(COLLECT_SET(status) OVER (PARTITION BY policy_id)) AS distinct_status_in_policy_id,
        ROUND(claimed_amount - AVG(claimed_amount) OVER (PARTITION BY policy_id), 2) AS claimed_amount_vs_policy_id_avg,
        SUM(approved_amount) OVER (PARTITION BY policy_id) AS policy_id_sum_approved_amount,
        ROUND(AVG(claimed_amount) OVER (PARTITION BY policy_id), 2) AS policy_id_avg_claimed_amount
    FROM with_stats
),
annotated AS (
    -- Label and score rows
    SELECT
        id,
        policy_id,
        claim_date,
        claim_type,
        claimed_amount,
        status,
        reported_at,
        status_prefix,
        status_len,
        last_claim_date,
        claimed_amount_vs_policy_id_avg,
        policy_id_sum_approved_amount,
        policy_id_avg_claimed_amount,
        TO_DATE(reported_at) AS reported_at_date,
        GREATEST(claimed_amount, status_len) AS greatest_claimed_amount_status_len,
        COALESCE(claim_type, 'UNKNOWN') AS claim_type_or_unknown
    FROM with_windows
    WHERE last_claim_date_2 IS NOT NULL
),
with_parent AS (
    -- Attach policy attributes
    SELECT
        s.id,
        s.policy_id,
        s.claim_date,
        s.claim_type,
        s.claimed_amount,
        s.status,
        s.reported_at,
        s.status_prefix,
        s.status_len,
        s.claimed_amount_vs_policy_id_avg,
        s.policy_id_sum_approved_amount,
        s.policy_id_avg_claimed_amount,
        s.reported_at_date,
        s.greatest_claimed_amount_status_len,
        s.claim_type_or_unknown,
        p.product AS policy_product,
        p.status AS policy_status,
        po.region AS holder_region
    FROM annotated s
    LEFT JOIN policies p ON p.id = s.policy_id
    LEFT JOIN policyholders po ON po.id = p.holder_id
),
latest AS (
    -- Keep the latest row per status
    SELECT
        id,
        policy_id,
        claim_date,
        claim_type,
        claimed_amount,
        status,
        reported_at,
        status_prefix,
        status_len,
        claimed_amount_vs_policy_id_avg,
        policy_id_sum_approved_amount,
        policy_id_avg_claimed_amount,
        reported_at_date,
        greatest_claimed_amount_status_len,
        claim_type_or_unknown,
        policy_product,
        policy_status,
        holder_region
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY status ORDER BY greatest_claimed_amount_status_len DESC, id DESC) AS rn
        FROM with_parent
    ) ranked
    WHERE rn = 1
),
detail_rows AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.claim_date,
        s.status,
        s.status_len,
        s.claimed_amount_vs_policy_id_avg,
        s.policy_id_sum_approved_amount,
        s.policy_id_avg_claimed_amount,
        s.reported_at_date,
        s.claim_type_or_unknown,
        s.policy_status,
        c.id AS claim_id,
        c.approved_amount AS claim_approved_amount,
        c.claim_date AS claim_claim_date
    FROM latest s
    JOIN claims c ON c.policy_id = s.policy_id AND c.approved_amount <= 28426.9
)
-- Subtotals with ROLLUP
SELECT
    policy_status,
    claim_id,
    policy_id,
    COUNT(DISTINCT claim_type_or_unknown) AS distinct_claim_type_or_unknowns
FROM detail_rows
GROUP BY ROLLUP(policy_status, claim_id, policy_id)
