WITH prepared_holders AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_name,
        region,
        risk_score,
        joined_on,
        DATEDIFF(DATE '2025-06-30', joined_on) AS days_since_joined_on
    FROM policyholders
    WHERE region NOT IN ('South', 'Central')
      AND joined_on < DATE '2025-09-02'
)
SELECT id, joined_on
FROM prepared_holders
WHERE days_since_joined_on > 140
  AND region = 'South'
UNION ALL
SELECT id, joined_on
FROM policyholders
ORDER BY id
