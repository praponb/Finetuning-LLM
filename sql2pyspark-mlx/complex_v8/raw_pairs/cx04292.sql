WITH cleaned_holders AS (
    -- Scope the policyholders rows
    SELECT
        id,
        holder_name,
        region,
        risk_score,
        joined_on,
        segment,
        DATE_TRUNC('YEAR', joined_on) AS joined_on_year
    FROM policyholders
)
SELECT holder_name AS label
FROM cleaned_holders
WHERE holder_name IS NOT NULL
EXCEPT
SELECT segment
FROM policyholders
WHERE region <> 'North'
  AND risk_score > 0.3
ORDER BY label
