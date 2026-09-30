SELECT id AS ref_id, start_date, premium AS value
FROM policies
WHERE end_date BETWEEN DATE '2025-09-27' AND DATE '2026-11-25'
UNION ALL
SELECT id, joined_on, risk_score
FROM policyholders
WHERE segment IN ('RETAIL', 'VIP')
  AND joined_on BETWEEN DATE '2024-02-22' AND DATE '2025-09-02'
UNION ALL
SELECT id, joined_on, risk_score
FROM policyholders
