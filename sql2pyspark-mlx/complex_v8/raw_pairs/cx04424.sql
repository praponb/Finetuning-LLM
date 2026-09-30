WITH filtered AS (
    -- Scope the policyholders rows
    SELECT
        id,
        holder_name,
        joined_on,
        segment
    FROM policyholders
    WHERE segment = 'VIP'
      AND region IN ('West', 'Central')
),
stacked AS (
    -- Stack the branches
    SELECT
        id,
        region AS category,
        risk_score,
        'holder' AS origin
    FROM policyholders
    WHERE LENGTH(TRIM(holder_name)) >= 8
      AND segment NOT IN ('RETAIL', 'VIP')
    UNION
    SELECT
        adjuster_id,
        claim_type,
        claimed_amount,
        'claim' AS origin
    FROM claims
)
SELECT origin, COUNT(*) AS n_rows, SUM(id) AS total_id
FROM stacked
GROUP BY origin
ORDER BY origin
