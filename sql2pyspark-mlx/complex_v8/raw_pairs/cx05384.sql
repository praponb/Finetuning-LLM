WITH combined AS (
    -- Stack the branches
    SELECT claim_type, reported_at AS event_time, 'claim' AS record_type
    FROM claims
    UNION
    SELECT status, reported_at, 'claim_2' AS record_type
    FROM claims
    WHERE claimed_amount >= 14019.8
      AND claim_type IN ('theft', 'collision', 'fire')
)
SELECT record_type, COUNT(*) AS n_rows, MAX(event_time) AS latest_event_time
FROM combined
GROUP BY record_type
ORDER BY record_type
