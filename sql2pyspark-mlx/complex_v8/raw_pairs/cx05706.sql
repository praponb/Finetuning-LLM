WITH combined AS (
    -- Stack the branches
    SELECT
        'claim' AS record_type,
        claim_type AS category,
        claim_date,
        status
    FROM claims
    UNION
    SELECT
        'holder' AS record_type,
        holder_name,
        joined_on,
        region
    FROM policyholders
    WHERE region IS NOT NULL
)
SELECT record_type, COUNT(*) AS n_rows, MAX(claim_date) AS latest_claim_date
FROM combined
GROUP BY record_type
ORDER BY record_type
