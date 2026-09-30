WITH unioned AS (
    -- Stack the branches
    SELECT 'claim' AS source, claim_type, reported_at AS event_time
    FROM claims
    WHERE policy_id >= 20
      AND claim_type = 'theft'
    UNION ALL
    SELECT 'claim_2' AS source, claim_type, reported_at
    FROM claims
)
SELECT source, COUNT(*) AS n_rows, MAX(event_time) AS latest_event_time
FROM unioned
GROUP BY source
ORDER BY source
