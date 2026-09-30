WITH cleaned_claims AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        claimed_amount,
        approved_amount,
        status,
        DATE_FORMAT(reported_at, 'MMM yyyy') AS reported_at_label,
        ROUND(claimed_amount * 1.1, 2) AS claimed_amount_adj
    FROM claims
    WHERE (status <> 'IN_REVIEW' OR claim_type NOT IN ('collision', 'medical'))
),
tagged AS (
    -- Derive helper columns
    SELECT
        *,
        ABS(claimed_amount - 32441.2) AS claimed_amount_distance,
        INITCAP(TRIM(reported_at_label)) AS reported_at_label_clean
    FROM cleaned_claims
)
SELECT
    COALESCE(claim_type, 'ALL') AS claim_type,
    COALESCE(status, 'ALL') AS status,
    policy_id,
    ARRAY_SORT(COLLECT_SET(status)) AS status_list,
    ROUND(AVG(claimed_amount), 2) AS avg_claimed_amount
FROM tagged
GROUP BY CUBE(claim_type, status, policy_id)
ORDER BY claim_type NULLS LAST, status NULLS LAST, policy_id NULLS LAST
