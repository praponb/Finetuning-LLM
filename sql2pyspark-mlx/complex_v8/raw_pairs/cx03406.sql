WITH all_events AS (
    -- Stack the branches
    SELECT
        'claim' AS origin,
        id,
        status,
        claim_date
    FROM claims
    WHERE reported_at >= TIMESTAMP '2025-01-25 11:00:00'
      AND policy_id <= 17
    UNION
    SELECT
        'claim_2' AS origin,
        policy_id,
        claim_type,
        claim_date
    FROM claims
    WHERE approved_amount > 0.0
      AND claimed_amount > 20436.5
)
SELECT origin, COUNT(*) AS n_rows, SUM(id) AS total_id, MAX(claim_date) AS latest_claim_date
FROM all_events
GROUP BY origin
ORDER BY origin
