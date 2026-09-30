WITH unioned AS (
    -- Stack the branches
    SELECT
        claim_type AS label,
        reported_at,
        status,
        'claim' AS origin
    FROM claims
    WHERE reported_at < TIMESTAMP '2025-02-18 15:00:00'
      AND claim_date < DATE '2025-09-19'
    UNION
    SELECT
        claim_type,
        reported_at,
        status,
        'claim_2' AS origin
    FROM claims
    WHERE claim_date >= DATE '2024-08-01'
      AND reported_at >= TIMESTAMP '2025-02-06 15:00:00'
)
SELECT origin, COUNT(*) AS n_rows, MAX(reported_at) AS latest_reported_at
FROM unioned
GROUP BY origin
ORDER BY origin
