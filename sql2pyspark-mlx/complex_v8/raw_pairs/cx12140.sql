WITH all_events AS (
    -- Stack the branches
    SELECT id AS ref_id, premium AS value, holder_id
    FROM policies
    UNION
    SELECT id, claimed_amount, adjuster_id
    FROM claims
)
SELECT ref_id, COUNT(*) AS n_rows, SUM(value) AS total_value
FROM all_events
GROUP BY ref_id
ORDER BY ref_id
