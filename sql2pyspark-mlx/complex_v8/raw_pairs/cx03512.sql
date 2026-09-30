WITH base_holders AS (
    -- Filter policyholders
    SELECT
        id,
        holder_name,
        region,
        risk_score,
        segment,
        CASE WHEN risk_score >= 0.6 THEN 'A' WHEN risk_score >= 0.2 THEN 'B' ELSE 'C' END AS risk_score_tier
    FROM policyholders
    WHERE joined_on < DATE '2025-04-20'
      AND region IN ('North', 'South')
)
SELECT region AS category, risk_score AS amount, 'holder' AS src
FROM base_holders
WHERE risk_score <= 0.8
UNION ALL
SELECT status AS category, claimed_amount AS amount, 'claim' AS src
FROM claims
WHERE policy_id IS NOT NULL
  AND adjuster_id >= 4
UNION ALL
SELECT claim_type AS category, approved_amount AS amount, 'claim' AS src
FROM claims
WHERE status = 'IN_REVIEW'
  AND adjuster_id <= 3
ORDER BY category, amount
