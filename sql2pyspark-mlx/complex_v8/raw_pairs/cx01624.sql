WITH stacked AS (
    -- Stack the branches
    SELECT policy_id AS qty, reported_at
    FROM claims
    UNION ALL
    SELECT policy_id, reported_at
    FROM claims
    WHERE approved_amount <= 20843.1
    UNION ALL
    SELECT adjuster_id, reported_at
    FROM claims
    WHERE HOUR(reported_at) BETWEEN 9 AND 14
)
SELECT qty, COUNT(*) AS n_rows, MAX(reported_at) AS latest_reported_at
FROM stacked
GROUP BY qty
ORDER BY qty
