WITH combined AS (
    -- Stack the branches
    SELECT claim_type, id AS qty, reported_at
    FROM claims
    WHERE YEAR(claim_date) = 2024
    UNION ALL
    SELECT status, policy_id, reported_at
    FROM claims
)
SELECT claim_type, COUNT(*) AS n_rows, SUM(qty) AS total_qty, MAX(reported_at) AS latest_reported_at
FROM combined
GROUP BY claim_type
ORDER BY claim_type
