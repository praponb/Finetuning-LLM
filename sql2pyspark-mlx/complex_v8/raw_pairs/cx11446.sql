WITH cleaned_claims AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        status,
        reported_at
    FROM claims
    WHERE claim_type NOT IN ('delay', 'collision')
      AND adjuster_id >= 2
)
SELECT status AS label, claim_type
FROM cleaned_claims
UNION ALL
SELECT segment, holder_name
FROM policyholders
WHERE UPPER(TRIM(holder_name)) LIKE 'D%'
  AND segment NOT IN ('RETAIL', 'VIP')
ORDER BY claim_type DESC, label
