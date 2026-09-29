WITH cleaned_claims AS (
    -- Scope the claims rows
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
        LOWER(REGEXP_REPLACE(TRIM(status), ' ', '_')) AS status_slug,
        COALESCE(claimed_amount, 0.0) AS claimed_amount_filled
    FROM claims
    WHERE (approved_amount BETWEEN 0.0 AND 20843.1 OR policy_id <= 10)
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SIZE(COLLECT_SET(policy_id) OVER (PARTITION BY adjuster_id, status)) AS distinct_policy_id_in_adjuster_id_status,
        FIRST_VALUE(claimed_amount) IGNORE NULLS OVER (PARTITION BY adjuster_id, policy_id ORDER BY claim_date NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_claimed_amount
    FROM cleaned_claims
),
detail_rows AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.status,
        s.distinct_policy_id_in_adjuster_id_status,
        c.id AS claim_id,
        c.approved_amount AS claim_approved_amount,
        c.claimed_amount AS claim_claimed_amount,
        c.status AS claim_status
    FROM sequenced s
    JOIN claims c ON c.adjuster_id = s.adjuster_id
),
annotated AS (
    -- Add calculated columns
    SELECT
        *,
        CASE WHEN status IN ('OPEN', 'PAID') THEN 'group_a' WHEN status = 'REJECTED' THEN 'group_b' ELSE 'other' END AS status_group,
        LEAST(claim_approved_amount, claim_claimed_amount) AS least_claim_approved_amount_claim_claimed_amount
    FROM detail_rows
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(AVG(claim_claimed_amount) OVER (PARTITION BY adjuster_id, policy_id ORDER BY distinct_policy_id_in_adjuster_id_status DESC, id, claim_id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS claim_claimed_amount_avg_7,
        ROUND(claim_claimed_amount / NULLIF(SUM(claim_claimed_amount) OVER (PARTITION BY policy_id), 0), 4) AS claim_claimed_amount_share_of_policy_id,
        COUNT(*) OVER (PARTITION BY policy_id) AS rows_in_policy_id
    FROM annotated
    WINDOW w_policy_id AS (PARTITION BY policy_id)
)
-- Final projection
SELECT
    id,
    policy_id,
    adjuster_id,
    claim_date,
    status,
    distinct_policy_id_in_adjuster_id_status,
    claim_id,
    claim_approved_amount,
    claim_status,
    status_group,
    least_claim_approved_amount_claim_claimed_amount,
    claim_claimed_amount_share_of_policy_id,
    rows_in_policy_id
FROM with_trends
WHERE claim_claimed_amount <= 42447.2
ORDER BY id DESC
