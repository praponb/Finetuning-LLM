WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        approved_amount,
        status,
        reported_at,
        LENGTH(TRIM(status)) AS status_len,
        DATE_ADD(claim_date, 90) AS claim_date_plus_90d,
        CASE WHEN claimed_amount >= 28900.9 THEN 'large' WHEN claimed_amount >= 11235.6 THEN 'medium' ELSE 'small' END AS claimed_amount_tier
    FROM claims
    WHERE status IN ('PAID', 'IN_REVIEW')
),
screened AS (
    -- Rows with no matching claims
    SELECT s.*
    FROM recent s
    WHERE NOT EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.policy_id
              AND c.adjuster_id IS NOT NULL
        )
),
latest_per_key AS (
    -- Keep the earliest row per status, claim_type
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        approved_amount,
        status,
        reported_at,
        status_len,
        claim_date_plus_90d,
        claimed_amount_tier
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY status, claim_type ORDER BY status_len, id) AS rn
        FROM screened
    ) ranked
    WHERE rn = 1
)
-- Subtotals with ROLLUP
SELECT
    claim_type,
    policy_id,
    COUNT(DISTINCT adjuster_id) AS distinct_adjuster_ids,
    SUM(CASE WHEN status = 'REJECTED' THEN 1 ELSE 0 END) AS n_status_rejected
FROM latest_per_key
GROUP BY ROLLUP(claim_type, policy_id)
ORDER BY claim_type NULLS LAST, policy_id NULLS LAST
