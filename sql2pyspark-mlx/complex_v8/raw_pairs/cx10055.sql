WITH all_events AS (
    -- Stack the branches
    SELECT
        id AS entity_id,
        claim_date AS day,
        status AS label,
        'claim' AS record_type
    FROM claims
    WHERE claim_type <> 'theft'
      AND claimed_amount BETWEEN 4554.8 AND 30115.9
    UNION ALL
    SELECT
        adjuster_id,
        claim_date,
        status,
        'claim_2' AS record_type
    FROM claims
    WHERE claim_date >= DATE '2024-06-20'
      AND adjuster_id IS NOT NULL
    UNION ALL
    SELECT
        id,
        claim_date,
        status,
        'claim_3' AS record_type
    FROM claims
    WHERE claimed_amount BETWEEN 6745.1 AND 40639.7
      AND YEAR(claim_date) = 2024
)
SELECT record_type, COUNT(*) AS n_rows, SUM(entity_id) AS total_entity_id, MAX(day) AS latest_day
FROM all_events
GROUP BY record_type
ORDER BY record_type
