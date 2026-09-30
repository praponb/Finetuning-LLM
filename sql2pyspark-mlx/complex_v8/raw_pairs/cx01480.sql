WITH filtered AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        approved_amount,
        status,
        FLOOR(approved_amount / 10000) * 10000 AS approved_amount_bucket,
        LOWER(TRIM(claim_type)) AS claim_type_clean,
        ABS(approved_amount - 2887.3) AS approved_amount_distance
    FROM claims
    WHERE claimed_amount BETWEEN 10673.3 AND 46461.4
      AND approved_amount BETWEEN 0.0 AND 32510.7
),
metrics AS (
    -- Previous / next values per key
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        approved_amount,
        status,
        approved_amount_bucket,
        claim_type_clean,
        approved_amount_distance,
        ROW_NUMBER() OVER w_status_adjuster_id_by_claime AS claimed_amount_seq,
        LEAD(claim_date) OVER w_status_policy_id_by_approved AS next_claim_date
    FROM filtered
    WINDOW w_status_adjuster_id_by_claime AS (PARTITION BY status, adjuster_id ORDER BY claimed_amount, id), w_status_policy_id_by_approved AS (PARTITION BY status, policy_id ORDER BY approved_amount_distance DESC, id)
),
enriched AS (
    -- Join policies
    SELECT s.*, p.status AS policy_status
    FROM metrics s
    LEFT JOIN policies p ON p.id = s.policy_id
)
SELECT
    COALESCE(status, 'ALL') AS status,
    policy_id,
    SUM(CASE WHEN status NOT IN ('IN_REVIEW', 'APPROVED') THEN 1 ELSE 0 END) AS n_status_not_in_in_review_appr,
    MIN(approved_amount_bucket) AS min_approved_amount_bucket,
    MIN(claim_date) AS first_claim_date
FROM enriched
GROUP BY GROUPING SETS ((status, policy_id), (status))
ORDER BY status NULLS LAST, policy_id NULLS LAST
