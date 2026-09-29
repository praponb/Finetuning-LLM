WITH recent_claims AS (
    -- Filter claims
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
        CONCAT(claim_type, '#', CAST(policy_id AS STRING)) AS claim_type_ref
    FROM claims
    WHERE (status <> 'IN_REVIEW' OR adjuster_id IS NOT NULL)
),
filtered AS (
    -- Keep rows whose adjuster matches
    SELECT s.*
    FROM recent_claims s
    WHERE EXISTS (
            SELECT 1
            FROM adjusters a
            WHERE a.id = s.adjuster_id
              AND LENGTH(TRIM(a.adjuster_name)) >= 6
              AND a.team = 'Health'
        )
)
SELECT
    status,
    claim_type,
    GROUPING(status) AS is_total_status,
    COUNT(*) AS n_rows,
    SUM(approved_amount) AS total_approved_amount
FROM filtered
GROUP BY GROUPING SETS ((status, claim_type), (status))
ORDER BY status NULLS LAST, claim_type NULLS LAST
