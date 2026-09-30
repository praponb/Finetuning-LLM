WITH prepared_holders AS (
    -- Filter policyholders
    SELECT
        id,
        region,
        risk_score,
        joined_on,
        segment,
        DATE_TRUNC('YEAR', joined_on) AS joined_on_year
    FROM policyholders
    WHERE region = 'South'
      AND segment IN ('VIP', 'SME')
),
unioned AS (
    -- Stack the branches
    SELECT segment AS label, region
    FROM prepared_holders
    WHERE region NOT IN ('North', 'Central')
      AND risk_score <= 0.6
    UNION ALL
    SELECT product AS label, status AS region
    FROM policies
    WHERE holder_id IS NOT NULL
      AND product = 'HEALTH'
)
SELECT label, COUNT(*) AS n_rows
FROM unioned
GROUP BY label
ORDER BY label
