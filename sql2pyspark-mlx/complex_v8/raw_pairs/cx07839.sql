WITH combined AS (
    -- Stack the branches
    SELECT
        'claim' AS source,
        status,
        reported_at AS event_time,
        claim_type
    FROM claims
    WHERE policy_id <= 8
      AND status = 'APPROVED'
    UNION ALL
    SELECT
        'claim_2' AS source,
        claim_type,
        reported_at,
        status
    FROM claims
    WHERE claim_type = 'flood'
)
SELECT source, COUNT(*) AS n_rows, MAX(event_time) AS latest_event_time
FROM combined
GROUP BY source
ORDER BY source
