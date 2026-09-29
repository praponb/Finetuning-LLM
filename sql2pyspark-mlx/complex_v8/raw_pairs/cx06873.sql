WITH scoped AS (
    -- Scope the policyholders rows
    SELECT
        id,
        holder_name,
        risk_score,
        joined_on,
        segment,
        QUARTER(joined_on) AS joined_on_quarter
    FROM policyholders
    WHERE risk_score < 0.8
)
SELECT joined_on_quarter AS qty
FROM scoped
WHERE risk_score > 0.2
  AND segment = 'SME'
INTERSECT
SELECT adjuster_id AS qty
FROM claims
WHERE claimed_amount >= 10673.3
ORDER BY qty
