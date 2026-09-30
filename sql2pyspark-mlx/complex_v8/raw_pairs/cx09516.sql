SELECT claim_type AS label, claim_date AS day, claimed_amount
FROM claims
WHERE HOUR(reported_at) BETWEEN 7 AND 12
  AND approved_amount BETWEEN 2349.3 AND 17759.3
UNION ALL
SELECT segment, joined_on, risk_score
FROM policyholders
WHERE joined_on BETWEEN DATE '2024-02-22' AND DATE '2025-10-17'
  AND UPPER(TRIM(holder_name)) LIKE 'A%'
ORDER BY day DESC, label
