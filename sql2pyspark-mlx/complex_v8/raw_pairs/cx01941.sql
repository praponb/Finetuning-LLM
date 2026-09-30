WITH combined AS (
    -- Stack the branches
    SELECT
        adjuster_id AS qty,
        reported_at,
        claimed_amount,
        'claim' AS record_type
    FROM claims
    WHERE status <> 'OPEN'
    UNION ALL
    SELECT
        id AS qty,
        reported_at,
        claimed_amount,
        'claim_2' AS record_type
    FROM claims
)
SELECT record_type, COUNT(*) AS n_rows, SUM(qty) AS total_qty, MAX(reported_at) AS latest_reported_at
FROM combined
GROUP BY record_type
ORDER BY record_type
