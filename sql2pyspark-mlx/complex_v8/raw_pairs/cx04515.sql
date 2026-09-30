WITH all_events AS (
    -- Stack the branches
    SELECT claim_type, status, claimed_amount AS value
    FROM claims
    UNION ALL
    SELECT claim_type, status, claimed_amount AS value
    FROM claims
)
SELECT claim_type, COUNT(*) AS n_rows, SUM(value) AS total_value
FROM all_events
GROUP BY claim_type
ORDER BY claim_type
