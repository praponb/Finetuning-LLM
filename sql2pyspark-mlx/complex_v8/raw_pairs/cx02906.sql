WITH stacked AS (
    -- Stack the branches
    SELECT status AS category, claim_date, approved_amount AS value
    FROM claims
    UNION
    SELECT segment, joined_on, risk_score
    FROM policyholders
)
SELECT category, COUNT(*) AS n_rows, SUM(value) AS total_value, MAX(claim_date) AS latest_claim_date
FROM stacked
GROUP BY category
ORDER BY category
