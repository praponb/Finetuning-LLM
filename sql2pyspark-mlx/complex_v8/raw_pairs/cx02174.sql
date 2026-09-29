WITH cleaned_holders AS (
    -- Scope the policyholders rows
    SELECT
        id,
        holder_name,
        region,
        risk_score,
        segment,
        CASE WHEN region IN ('West', 'Central') THEN 'group_a' WHEN region = 'South' THEN 'group_b' ELSE 'other' END AS region_group
    FROM policyholders
    WHERE (segment = 'SME' OR region IS NOT NULL)
)
SELECT id, region_group
FROM cleaned_holders
EXCEPT ALL
SELECT policy_id AS id, claim_type AS region_group
FROM claims
WHERE claim_date < DATE '2025-01-24'
  AND adjuster_id IS NOT NULL
