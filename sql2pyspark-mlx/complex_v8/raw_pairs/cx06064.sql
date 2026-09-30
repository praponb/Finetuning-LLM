WITH all_events AS (
    -- Stack the branches
    SELECT 'holder' AS source, holder_name, segment
    FROM policyholders
    WHERE YEAR(joined_on) = 2024
      AND segment <> 'VIP'
    UNION ALL
    SELECT 'claim' AS source, status, claim_type
    FROM claims
)
SELECT source, COUNT(*) AS n_rows
FROM all_events
GROUP BY source
ORDER BY source
