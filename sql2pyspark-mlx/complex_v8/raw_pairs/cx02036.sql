WITH cleaned_claims AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        claimed_amount,
        approved_amount,
        reported_at,
        FLOOR(claimed_amount / 10000) * 10000 AS claimed_amount_bucket
    FROM claims
    WHERE (adjuster_id >= 4 OR YEAR(claim_date) = 2024)
),
derived AS (
    -- Label and score rows
    SELECT
        *,
        CASE WHEN claimed_amount_bucket >= 35030 THEN 'A' WHEN claimed_amount_bucket >= 20060 THEN 'B' ELSE 'C' END AS claimed_amount_bucket_tier,
        CASE WHEN claim_type IN ('collision', 'medical') THEN 'group_a' WHEN claim_type = 'fire' THEN 'group_b' ELSE 'other' END AS claim_type_group,
        DATEDIFF(DATE '2025-12-31', reported_at) AS days_since_reported_at
    FROM cleaned_claims
)
SELECT
    COALESCE(claim_type_group, 'ALL') AS claim_type_group,
    COALESCE(claim_type, 'ALL') AS claim_type,
    COALESCE(claimed_amount_bucket_tier, 'ALL') AS claimed_amount_bucket_tier,
    GROUPING_ID() AS grouping_level,
    ROUND(SUM(claimed_amount) / NULLIF(SUM(days_since_reported_at), 0), 3) AS claimed_amount_to_days_since_reported_at_ratio
FROM derived
GROUP BY GROUPING SETS ((claim_type_group, claim_type, claimed_amount_bucket_tier), (claim_type_group), (claim_type))
