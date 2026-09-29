WITH cleaned_holders AS (
    -- Filter policyholders
    SELECT
        id,
        holder_name,
        risk_score,
        joined_on,
        ABS(risk_score - 0.2) AS risk_score_distance
    FROM policyholders
    WHERE UPPER(TRIM(holder_name)) LIKE 'A%'
)
SELECT id AS ref_id, joined_on AS day
FROM cleaned_holders
UNION ALL
SELECT policy_id, claim_date
FROM claims
WHERE HOUR(reported_at) BETWEEN 10 AND 15
  AND claimed_amount BETWEEN 13635.1 AND 29985.9
