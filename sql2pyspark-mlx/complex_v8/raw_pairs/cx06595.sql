WITH recent_claims AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        approved_amount,
        status,
        reported_at,
        UPPER(TRIM(status)) AS status_clean,
        CONCAT_WS(' / ', claim_type, status) AS claim_type_status_label
    FROM claims
    WHERE claimed_amount < 34763.4
),
annotated AS (
    -- Label and score rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        reported_at,
        FLOOR(approved_amount / 10000) * 10000 AS approved_amount_bucket,
        CASE WHEN status IN ('APPROVED', 'IN_REVIEW') THEN 'group_a' WHEN status = 'PAID' THEN 'group_b' ELSE 'other' END AS status_group,
        LOWER(REGEXP_REPLACE(TRIM(status), ' ', '_')) AS status_slug
    FROM recent_claims
    WHERE claim_type_status_label IS NOT NULL
),
exploded_detail_claims AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.approved_amount_bucket,
        s.status_slug,
        c.id AS claim_id,
        c.policy_id AS claim_policy_id,
        c.status AS claim_status
    FROM annotated s
    LEFT JOIN claims c ON c.adjuster_id = s.adjuster_id AND c.status IN ('IN_REVIEW', 'PAID')
),
without_matches AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM exploded_detail_claims s
    WHERE s.policy_id IN (
            SELECT c.policy_id
            FROM claims c
            WHERE c.adjuster_id IS NOT NULL
              AND c.claim_date < DATE '2024-09-08'
        )
      AND s.status_slug IS NOT NULL
)
SELECT
    adjuster_id,
    policy_id,
    SUM(CASE WHEN claim_status IN ('OPEN', 'PAID', 'APPROVED') THEN 1 ELSE 0 END) AS n_claim_status_in_open_paid_ap,
    MIN(claim_date) AS first_claim_date
FROM without_matches
GROUP BY ROLLUP(adjuster_id, policy_id)
