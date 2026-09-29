WITH recent_claims AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        approved_amount,
        reported_at,
        DATE_FORMAT(reported_at, 'MMM yyyy') AS reported_at_label
    FROM claims
    WHERE HOUR(reported_at) BETWEEN 9 AND 11
),
claim_stats AS (
    -- Per-adjuster claims metrics
    SELECT
        adjuster_id,
        COUNT(*) FILTER (WHERE status NOT IN ('APPROVED', 'OPEN')) AS claim_status_count,
        COUNT(CASE WHEN status IS NOT NULL THEN 1 END) AS claim_status_count_2
    FROM claims
    GROUP BY adjuster_id
),
summarized_rows AS (
    -- Attach claims metrics
    SELECT
        s.*,
        COALESCE(c.claim_status_count, 0) AS claim_status_count,
        COALESCE(c.claim_status_count_2, 0) AS claim_status_count_2
    FROM recent_claims s
    LEFT JOIN claim_stats c ON c.adjuster_id = s.adjuster_id
),
tagged AS (
    -- Add calculated columns
    SELECT
        *,
        ROUND(claim_status_count / NULLIF(approved_amount, 0), 3) AS claim_status_count_per_approved_amount,
        ROUND(claimed_amount / NULLIF(claim_status_count_2, 0), 3) AS claimed_amount_per_claim_status_count_2,
        LOWER(REGEXP_REPLACE(TRIM(reported_at_label), ' ', '_')) AS reported_at_label_slug
    FROM summarized_rows
    WHERE YEAR(claim_date) = 2025
)
-- Final projection
SELECT
    id,
    policy_id,
    adjuster_id,
    claim_date,
    claimed_amount,
    approved_amount,
    reported_at_label,
    claim_status_count,
    claim_status_count_2,
    claim_status_count_per_approved_amount,
    claimed_amount_per_claim_status_count_2,
    reported_at_label_slug
FROM tagged
ORDER BY claim_status_count DESC, claimed_amount
