WITH scoped_claims AS (
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
        CONCAT(status, '#', CAST(policy_id AS STRING)) AS status_ref,
        CASE WHEN status IN ('OPEN', 'PAID') THEN 'group_a' WHEN status = 'IN_REVIEW' THEN 'group_b' ELSE 'other' END AS status_group,
        CASE WHEN approved_amount >= 15857.9 THEN 'A' WHEN approved_amount >= 492.6 THEN 'B' ELSE 'C' END AS approved_amount_tier
    FROM claims
    WHERE claim_date IS NOT NULL
),
stats AS (
    -- Summarise per group
    SELECT
        TO_DATE(reported_at) AS reported_at_day,
        COUNT(*) AS cnt,
        COUNT(DISTINCT status_group) AS distinct_status_groups,
        ROUND(SUM(approved_amount), 2) AS total_approved_amount,
        SUM(CASE WHEN claim_type NOT IN ('medical', 'delay') THEN 1 ELSE 0 END) AS n_claim_type_not_in_medical_de,
        ROUND(SUM(approved_amount) / NULLIF(SUM(claimed_amount), 0), 3) AS approved_amount_to_claimed_amount_ratio,
        COUNT(*) FILTER (WHERE claim_type = 'collision') AS n_claim_type_collision
    FROM scoped_claims
    GROUP BY 1
),
qualified AS (
    -- Compare against the overall avg
    SELECT *
    FROM stats
    WHERE n_claim_type_not_in_medical_de > (SELECT AVG(n_claim_type_not_in_medical_de) FROM stats)
)
SELECT reported_at_day, measure, reading
FROM (
    SELECT reported_at_day, approved_amount_to_claimed_amount_ratio, total_approved_amount
    FROM qualified
) src
UNPIVOT (
    reading FOR measure IN (approved_amount_to_claimed_amount_ratio AS m_approved_amount_to_claimed_amount_ratio, total_approved_amount AS total_approved_amount_v)
) unp
