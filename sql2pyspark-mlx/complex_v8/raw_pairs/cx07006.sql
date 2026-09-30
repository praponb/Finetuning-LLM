WITH all_events AS (
    -- Stack the branches
    SELECT 'holder' AS record_type, segment AS category, joined_on
    FROM policyholders
    WHERE holder_name IS NOT NULL
    UNION ALL
    SELECT 'policy' AS record_type, product, end_date
    FROM policies
    WHERE end_date IS NOT NULL
      AND product NOT IN ('HEALTH', 'AUTO')
)
SELECT record_type, COUNT(*) AS n_rows, MAX(joined_on) AS latest_joined_on
FROM all_events
GROUP BY record_type
ORDER BY record_type
