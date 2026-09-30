WITH scoped_claims AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        status,
        reported_at,
        FLOOR(approved_amount / 10000) * 10000 AS approved_amount_bucket,
        CASE WHEN approved_amount >= 23120.7 THEN 'A' WHEN approved_amount >= 2349.3 THEN 'B' ELSE 'C' END AS approved_amount_tier
    FROM claims
    WHERE reported_at < TIMESTAMP '2025-03-02 12:00:00'
      AND policy_id >= 5
),
annotated AS (
    -- Derive helper columns
    SELECT
        *,
        COALESCE(approved_amount_bucket, 0) AS approved_amount_bucket_filled,
        UPPER(TRIM(status)) AS status_clean
    FROM scoped_claims
    WHERE TO_DATE(reported_at) < DATE '2025-02-18'
),
enriched AS (
    -- Join policies
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.reported_at,
        s.approved_amount_tier,
        p.end_date AS policy_end_date,
        a.adjuster_name
    FROM annotated s
    JOIN policies p ON p.id = s.policy_id
    LEFT JOIN adjusters a ON a.id = s.adjuster_id
)
SELECT
    id,
    policy_id,
    adjuster_id,
    reported_at,
    approved_amount_tier,
    policy_end_date,
    adjuster_name,
    SUBSTRING(TRIM(approved_amount_tier), 1, 3) AS approved_amount_tier_prefix
FROM enriched
WHERE approved_amount_tier <> 'A'
ORDER BY adjuster_id, reported_at DESC
