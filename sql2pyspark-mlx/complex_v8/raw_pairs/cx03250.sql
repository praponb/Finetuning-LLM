WITH unioned AS (
    -- Stack the branches
    SELECT status AS label, start_date AS event_date, 'policy' AS source
    FROM policies
    UNION ALL
    SELECT claim_type, claim_date, 'claim' AS source
    FROM claims
)
SELECT source, COUNT(*) AS n_rows, MAX(event_date) AS latest_event_date
FROM unioned
GROUP BY source
ORDER BY source
