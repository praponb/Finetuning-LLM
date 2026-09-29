WITH scoped_holders AS (
    -- Filter policyholders
    SELECT
        id,
        holder_name,
        region,
        risk_score,
        joined_on
    FROM policyholders
    WHERE YEAR(joined_on) = 2025
),
unioned AS (
    -- Stack the branches
    SELECT holder_name, risk_score AS amount
    FROM scoped_holders
    WHERE joined_on >= DATE '2024-06-18'
      AND risk_score >= 0.2
    UNION ALL
    SELECT segment AS holder_name, risk_score AS amount
    FROM policyholders
)
SELECT holder_name, COUNT(*) AS n_rows, SUM(amount) AS total_amount
FROM unioned
GROUP BY holder_name
ORDER BY holder_name
