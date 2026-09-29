WITH stacked AS (
    -- Stack the branches
    SELECT
        'claim' AS record_type,
        adjuster_id,
        claim_date AS day,
        reported_at AS event_time
    FROM claims
    UNION ALL
    SELECT
        'claim_2' AS record_type,
        policy_id AS adjuster_id,
        claim_date AS day,
        reported_at AS event_time
    FROM claims
    UNION ALL
    SELECT
        'claim_3' AS record_type,
        id AS adjuster_id,
        claim_date AS day,
        reported_at AS event_time
    FROM claims
)
SELECT record_type, COUNT(*) AS n_rows, SUM(adjuster_id) AS total_adjuster_id, MAX(day) AS latest_day
FROM stacked
GROUP BY record_type
ORDER BY record_type
